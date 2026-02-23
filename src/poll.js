import { createClient } from "@supabase/supabase-js";

const INTERCOM_BASE_URL = "https://api.intercom.io";

// Safety window:
// Every run: fetch convos updated since (last_run - lookback) to now
const LOOKBACK_MINUTES = 45; // > 10 min to cover delays/retries
const SEARCH_PER_PAGE = 50;
const MAX_CONVERSATIONS_PER_RUN = 500;

const LIVE_LAST_RUN_KEY = "live_last_run_iso";
const LIVE_CURSOR_KEY = "live_starting_after";

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const SUPABASE_URL = requireEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = requireEnv("SUPABASE_SERVICE_ROLE_KEY");
const INTERCOM_ACCESS_TOKEN = requireEnv("INTERCOM_ACCESS_TOKEN");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

async function getState(key) {
  const { data, error } = await supabase
    .from("sync_state")
    .select("value")
    .eq("key", key)
    .maybeSingle();
  if (error) throw error;
  return data?.value ?? "";
}

async function setState(key, value) {
  const { error } = await supabase
    .from("sync_state")
    .upsert({ key, value: String(value) }, { onConflict: "key" });
  if (error) throw error;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function intercomRequest(method, path, bodyOrNull) {
  const url = `${INTERCOM_BASE_URL}${path}`;
  const maxAttempts = 6;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: bodyOrNull ? JSON.stringify(bodyOrNull) : undefined,
    });

    const text = await res.text();
    if (res.ok) return text ? JSON.parse(text) : null;

    if (res.status === 429 || (res.status >= 500 && res.status <= 599)) {
      const backoffMs = Math.min(30000, 500 * Math.pow(2, attempt));
      console.log(`Retryable ${res.status} attempt ${attempt}, sleep ${backoffMs}ms`);
      await sleep(backoffMs);
      continue;
    }

    console.log(`Intercom error ${res.status}: ${text.slice(0, 2000)}`);
    return null;
  }

  return null;
}

async function searchConversationsUpdatedBetween(startUnix, endUnix, perPage, startingAfter) {
  const body = {
    query: {
      operator: "AND",
      value: [
        { field: "updated_at", operator: ">=", value: startUnix },
        { field: "updated_at", operator: "<=", value: endUnix },
      ],
    },
    sort: { field: "updated_at", order: "ascending" },
    pagination: { per_page: perPage, ...(startingAfter ? { starting_after: startingAfter } : {}) },
  };
  return intercomRequest("POST", "/conversations/search", body);
}

async function getConversation(conversationId) {
  return intercomRequest("GET", `/conversations/${encodeURIComponent(conversationId)}`, null);
}

function htmlToText(html) {
  if (!html) return "";
  let text = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"');
  return text.replace(/[ \t]+/g, " ").replace(/\n\s+/g, "\n").trim();
}

function normalizeMessage(obj) {
  const createdAt = obj?.created_at ? Number(obj.created_at) : null;
  const createdIso = createdAt ? new Date(createdAt * 1000).toISOString() : null;
  const author = obj?.author || {};
  const authorType = String(author?.type || "").toLowerCase() === "admin" ? "admin" : "user";
  return {
    part_id: obj?.id || null,
    created_at_iso: createdIso,
    author_type: authorType,
    author_id: author?.id ? String(author.id) : null,
    author_name: author?.name ? String(author.name) : null,
    body_text: htmlToText(obj?.body || ""),
  };
}

function buildOrderedMessages(convo) {
  const out = [];
  if (convo?.source) out.push(normalizeMessage(convo.source));
  const parts = convo?.conversation_parts?.conversation_parts || [];
  for (const p of parts) out.push(normalizeMessage(p));
  out.sort((a, b) => new Date(a.created_at_iso || 0) - new Date(b.created_at_iso || 0));
  return out;
}

function findPreviousUserMessage(messages, idx) {
  for (let j = idx - 1; j >= 0; j--) {
    const m = messages[j];
    if (m?.author_type === "user" && m?.body_text?.trim()) return m.body_text;
  }
  return null;
}

async function upsertReplies(rows) {
  if (!rows.length) return;
  const { error } = await supabase.from("replies").upsert(rows, { onConflict: "part_id" });
  if (error) throw error;
}

async function main() {
  // initialize last_run if missing
  let lastRunIso = await getState(LIVE_LAST_RUN_KEY);
  if (!lastRunIso) {
    lastRunIso = new Date(Date.now() - LOOKBACK_MINUTES * 60 * 1000).toISOString();
    await setState(LIVE_LAST_RUN_KEY, lastRunIso);
    await setState(LIVE_CURSOR_KEY, "");
  }

  const lastRun = new Date(lastRunIso);
  const start = new Date(lastRun.getTime() - LOOKBACK_MINUTES * 60 * 1000);
  const end = new Date();

  const startUnix = Math.floor(start.getTime() / 1000);
  const endUnix = Math.floor(end.getTime() / 1000);

  let startingAfter = (await getState(LIVE_CURSOR_KEY)) || null;

  let processed = 0;
  let pages = 0;
  let rowsUpserted = 0;

  while (processed < MAX_CONVERSATIONS_PER_RUN) {
    const searchResp = await searchConversationsUpdatedBetween(startUnix, endUnix, SEARCH_PER_PAGE, startingAfter);
    const convos = searchResp?.conversations || [];
    const nextCursor = searchResp?.pages?.next?.starting_after ? String(searchResp.pages.next.starting_after) : null;

    if (!convos.length) {
      await setState(LIVE_LAST_RUN_KEY, end.toISOString());
      await setState(LIVE_CURSOR_KEY, "");
      console.log("Poller: no conversations; advanced last_run.");
      return;
    }

    const rows = [];
    for (const c of convos) {
      if (processed >= MAX_CONVERSATIONS_PER_RUN) break;
      const convoId = c?.id;
      if (!convoId) continue;

      const full = await getConversation(convoId);
      if (!full) continue;

      const messages = buildOrderedMessages(full);
      const assigneeId = full?.admin_assignee_id ? String(full.admin_assignee_id) : null;

      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        if (msg?.author_type !== "admin") continue;
        if (!msg?.body_text?.trim()) continue;

        const userPrev = findPreviousUserMessage(messages, i);

        rows.push({
          pulled_at: new Date().toISOString(),
          conversation_id: String(convoId),
          part_id: String(msg.part_id),
          reply_created_at: msg.created_at_iso,
          teammate_id: msg.author_id,
          teammate_name: msg.author_name,
          tags: null,
          assignee_id: assigneeId,
          user_prev_message: userPrev,
          agent_reply: msg.body_text,
        });
      }

      processed++;
    }

    if (rows.length) {
      await upsertReplies(rows);
      rowsUpserted += rows.length;
    }

    pages++;
    startingAfter = nextCursor;
    await setState(LIVE_CURSOR_KEY, startingAfter || "");

    if (!nextCursor) {
      await setState(LIVE_LAST_RUN_KEY, end.toISOString());
      await setState(LIVE_CURSOR_KEY, "");
      break;
    }

    await sleep(200);
  }

  console.log(`Poller done: pages=${pages} convos=${processed} upsert_rows=${rowsUpserted}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
