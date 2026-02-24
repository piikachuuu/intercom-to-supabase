import { createClient } from "@supabase/supabase-js";

const INTERCOM_BASE_URL = "https://api.intercom.io";

// Backfill caps (tune as needed)
const SEARCH_PER_PAGE = 50;
const MAX_CONVERSATIONS_PER_RUN = 60;
const MAX_ROWS_INSERT_PER_RUN = 1500;

// Live caps
const LIVE_LOOKBACK_MINUTES = 30;
const LIVE_SEARCH_PER_PAGE = 50;
const LIVE_MAX_CONVERSATIONS_PER_RUN = 500;

// State keys
const BF_START_KEY = "bf_start_iso";
const BF_END_KEY = "bf_end_iso";
const BF_CURSOR_KEY = "bf_starting_after";
const BF_DONE_KEY = "bf_done";

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

// ---------- Supabase state helpers ----------
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

// ---------- Intercom helpers ----------
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
    const code = res.status;

    if (code >= 200 && code < 300) {
      try {
        return JSON.parse(text);
      } catch {
        return null;
      }
    }

    // Retry on rate limit / transient errors
    if (code === 429 || (code >= 500 && code <= 599)) {
      const backoffMs = Math.min(30000, 500 * Math.pow(2, attempt));
      console.log(
        `Intercom retryable error ${code} attempt ${attempt}/${maxAttempts}; sleep ${backoffMs}ms`
      );
      await sleep(backoffMs);
      continue;
    }

    console.log(`Intercom API error ${code} for ${url}: ${text.slice(0, 2000)}`);
    return null;
  }

  console.log(`Intercom API error: exhausted retries for ${path}`);
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

// ---------- Extraction ----------
function htmlToText(html) {
  if (!html) return "";
  let text = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<[^>]*>/g, " ");

  text = text
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
  const createdIso = createdAt ? new Date(createdAt * 1000).toISOString() : "";

  const author = obj?.author || {};
  const authorTypeRaw = String(author?.type || "").toLowerCase();
  const authorType = authorTypeRaw === "admin" ? "admin" : "user";

  const bodyHtml = obj?.body || "";
  const bodyText = htmlToText(bodyHtml);

  return {
    part_id: obj?.id || null,
    created_at: createdAt,
    created_at_iso: createdIso,
    author_type: authorType,
    author_id: author?.id ? String(author.id) : "",
    author_name: author?.name ? String(author.name) : "",
    body_text: bodyText,
  };
}

function buildOrderedMessages(conversation) {
  const out = [];
  if (conversation?.source) out.push(normalizeMessage(conversation.source));

  const parts = conversation?.conversation_parts?.conversation_parts || [];
  for (const p of parts) out.push(normalizeMessage(p));

  out.sort((a, b) => (a.created_at || 0) - (b.created_at || 0));
  return out;
}

function findPreviousUserMessage(messages, idx) {
  for (let j = idx - 1; j >= 0; j--) {
    const m = messages[j];
    if (m?.author_type === "user" && m?.body_text?.trim()) return m.body_text;
  }
  return "";
}

function extractTags(conversation) {
  const tagsObj = conversation?.tags;
  if (tagsObj?.tags && Array.isArray(tagsObj.tags)) {
    return tagsObj.tags.map((t) => t.name).filter(Boolean).join(", ");
  }
  if (Array.isArray(tagsObj)) {
    return tagsObj.map((t) => t.name).filter(Boolean).join(", ");
  }
  return "";
}

function extractAssigneeId(conversation) {
  if (conversation?.assignee?.id) return String(conversation.assignee.id);
  if (conversation?.admin_assignee_id) return String(conversation.admin_assignee_id);
  return "";
}

// ---------- DB insert (dedupe via unique index) ----------
async function insertReplies(rows) {
  if (!rows.length) return { inserted: 0 };

  // Use upsert with onConflict to avoid duplicates
  // "ignoreDuplicates" isn’t in supabase-js; this pattern is the most reliable:
  const { data, error } = await supabase
    .from("replies")
    .upsert(rows, { onConflict: "part_id" })
    .select("part_id");

  if (error) throw error;

  // data will include both inserted and updated, but we never update fields usually.
  // It’s still fine for “at least once” semantics.
  return { upserted: data?.length ?? 0 };
}

// ---------- Backfill initialization helper ----------
function mostRecentSundayStartISO(now = new Date()) {
  const start = new Date(now);
  const day = start.getDay(); // 0 = Sunday
  start.setHours(0, 0, 0, 0);
  start.setDate(start.getDate() - day);
  return start.toISOString();
}

async function initBackfillSundayToNowIfNeeded() {
  const startIso = await getState(BF_START_KEY);
  const endIso = await getState(BF_END_KEY);

  if (startIso && endIso) return;

  const now = new Date();
  await setState(BF_START_KEY, mostRecentSundayStartISO(now));
  await setState(BF_END_KEY, now.toISOString());
  await setState(BF_CURSOR_KEY, "");
  await setState(BF_DONE_KEY, "false");

  console.log(`Initialized backfill window Sunday->now`);
}

// ---------- Main: run once (either continues backfill, or runs live) ----------
async function runBackfillOnce() {
  await initBackfillSundayToNowIfNeeded();

  const done = String(await getState(BF_DONE_KEY)).toLowerCase() === "true";
  if (done) return { done: true };

  const startIso = await getState(BF_START_KEY);
  const endIso = await getState(BF_END_KEY);
  if (!startIso || !endIso) throw new Error("Backfill state missing start/end ISO.");

  const startUnix = Math.floor(new Date(startIso).getTime() / 1000);
  const endUnix = Math.floor(new Date(endIso).getTime() / 1000);

  let startingAfter = (await getState(BF_CURSOR_KEY)) || null;

  let processed = 0;
  let totalRows = 0;
  let pages = 0;

  while (processed < MAX_CONVERSATIONS_PER_RUN && totalRows < MAX_ROWS_INSERT_PER_RUN) {
    const searchResp = await searchConversationsUpdatedBetween(
      startUnix,
      endUnix,
      SEARCH_PER_PAGE,
      startingAfter
    );

    const convos = searchResp?.conversations || [];
    const nextStartingAfter =
      searchResp?.pages?.next?.starting_after ? String(searchResp.pages.next.starting_after) : null;

    if (!convos.length) {
      await setState(BF_DONE_KEY, "true");
      await setState(BF_CURSOR_KEY, "");
      console.log("Backfill: no conversations found; marked done.");
      return { done: true, pages, processed, totalRows };
    }

    const rows = [];

    for (const c of convos) {
      if (processed >= MAX_CONVERSATIONS_PER_RUN) break;
      if (totalRows + rows.length >= MAX_ROWS_INSERT_PER_RUN) break;

      const convoId = c?.id;
      if (!convoId) continue;

      const full = await getConversation(convoId);
      if (!full) continue;

      const tags = extractTags(full);
      const assigneeId = extractAssigneeId(full);
      const messages = buildOrderedMessages(full);

      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        if (msg?.author_type !== "admin") continue;
        if (!msg?.body_text?.trim()) continue;

        const partId = msg.part_id || `source_admin_${convoId}_${msg.created_at || i}`;
        const userPrev = findPreviousUserMessage(messages, i);

        rows.push({
          pulled_at: new Date().toISOString(),
          conversation_id: String(convoId),
          part_id: String(partId),
          reply_created_at: msg.created_at_iso || null,
          teammate_id: msg.author_id || null,
          teammate_name: msg.author_name || null,
          tags: tags || null,
          assignee_id: assigneeId || null,
          user_prev_message: userPrev || null,
          agent_reply: msg.body_text || null,
        });
      }

      processed++;
    }

    if (rows.length) {
      const { upserted } = await insertReplies(rows);
      totalRows += upserted;
    }

    pages++;
    startingAfter = nextStartingAfter;
    await setState(BF_CURSOR_KEY, startingAfter || "");

    console.log(
      `Backfill: pages=${pages}, processed_convos=${processed}, upserted_rows≈${totalRows}, next_cursor=${startingAfter || "none"}`
    );

    if (!nextStartingAfter) {
      await setState(BF_DONE_KEY, "true");
      await setState(BF_CURSOR_KEY, "");
      console.log("Backfill complete.");
      return { done: true, pages, processed, totalRows };
    }

    await sleep(200);
  }

  return { done: false, pages, processed, totalRows };
}

async function autoSwitchToLiveIfBackfillDone() {
  const done = String(await getState(BF_DONE_KEY)).toLowerCase() === "true";
  if (!done) return;

  const alreadyLive = !!(await getState(LIVE_LAST_RUN_KEY));
  if (alreadyLive) return;

  const bfEndIso = await getState(BF_END_KEY);
  const liveStart = bfEndIso ? new Date(bfEndIso) : new Date();

  await setState(LIVE_LAST_RUN_KEY, liveStart.toISOString());
  await setState(LIVE_CURSOR_KEY, "");
  console.log(`Auto-switch: initialized live_last_run_iso=${liveStart.toISOString()}`);
}

async function runLiveOnce() {
  const lastRunIso = await getState(LIVE_LAST_RUN_KEY);
  if (!lastRunIso) {
    console.log("Live: live_last_run_iso not set (waiting for backfill auto-switch).");
    return { ran: false };
  }

  const lastRun = new Date(lastRunIso);
  const start = new Date(lastRun.getTime() - LIVE_LOOKBACK_MINUTES * 60 * 1000);
  const end = new Date();

  const startUnix = Math.floor(start.getTime() / 1000);
  const endUnix = Math.floor(end.getTime() / 1000);

  let startingAfter = (await getState(LIVE_CURSOR_KEY)) || null;

  let processed = 0;
  let pages = 0;
  let totalRows = 0;

  while (processed < LIVE_MAX_CONVERSATIONS_PER_RUN) {
    const searchResp = await searchConversationsUpdatedBetween(
      startUnix,
      endUnix,
      LIVE_SEARCH_PER_PAGE,
      startingAfter
    );

    const convos = searchResp?.conversations || [];
    const nextStartingAfter =
      searchResp?.pages?.next?.starting_after ? String(searchResp.pages.next.starting_after) : null;

    if (!convos.length) {
      await setState(LIVE_LAST_RUN_KEY, end.toISOString());
      await setState(LIVE_CURSOR_KEY, "");
      console.log("Live: no new conversations. Advanced last_run.");
      return { ran: true, pages, processed, totalRows };
    }

    const rows = [];

    for (const c of convos) {
      if (processed >= LIVE_MAX_CONVERSATIONS_PER_RUN) break;

      const convoId = c?.id;
      if (!convoId) continue;

      const full = await getConversation(convoId);
      if (!full) continue;

      const tags = extractTags(full);
      const assigneeId = extractAssigneeId(full);
      const messages = buildOrderedMessages(full);

      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        if (msg?.author_type !== "admin") continue;
        if (!msg?.body_text?.trim()) continue;

        const partId = msg.part_id || `source_admin_${convoId}_${msg.created_at || i}`;
        const userPrev = findPreviousUserMessage(messages, i);

        rows.push({
          pulled_at: new Date().toISOString(),
          conversation_id: String(convoId),
          part_id: String(partId),
          reply_created_at: msg.created_at_iso || null,
          teammate_id: msg.author_id || null,
          teammate_name: msg.author_name || null,
          tags: tags || null,
          assignee_id: assigneeId || null,
          user_prev_message: userPrev || null,
          agent_reply: msg.body_text || null,
        });
      }

      processed++;
    }

    if (rows.length) {
      const { upserted } = await insertReplies(rows);
      totalRows += upserted;
    }

    pages++;
    startingAfter = nextStartingAfter;

    await setState(LIVE_CURSOR_KEY, startingAfter || "");

    if (!nextStartingAfter) {
      await setState(LIVE_LAST_RUN_KEY, end.toISOString());
      await setState(LIVE_CURSOR_KEY, "");
      break;
    }

    await sleep(200);
  }

  console.log(
    `Live: pages=${pages}, processed_convos=${processed}, upserted_rows≈${totalRows}, next_cursor=${startingAfter || "none"}`
  );

  return { ran: true, pages, processed, totalRows };
}

// ---------- Entrypoint ----------
async function main() {
  // 1) Try to advance backfill until done
  const bf = await runBackfillOnce();

  // 2) If backfill finished, initialize live if needed
  await autoSwitchToLiveIfBackfillDone();

  // 3) Always attempt one live pass (will no-op if not ready)
  const lv = await runLiveOnce();

  console.log("Done:", { backfill: bf, live: lv });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
