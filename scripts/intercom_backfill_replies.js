/**
 * scripts/intercom_backfill_replies.js
 *
 * Auto-draining backfill worker:
 * - Each run selects conversations from replies where any of:
 *   - conversation_state IS NULL
 *   - is_intercom_note IS NULL
 *   - user_prev_message_created_at IS NULL
 * - Fetch conversation from Intercom
 * - Rebuild rows for HUMAN ADMIN parts only (author.type === "admin")
 * - Upsert into public.replies onConflict: part_id
 * - After run, checks remaining rows; writes GitHub Actions outputs.
 *
 * Required env:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *   INTERCOM_ACCESS_TOKEN
 *
 * Optional env:
 *   LIMIT_REPLIES_SCAN (default 5000)       // how many reply rows to look at each run
 *   INTERCOM_CONCURRENCY (default 2)
 *   INTERCOM_SLEEP_MS (default 300)
 */

import { createClient } from "@supabase/supabase-js";
import fs from "node:fs";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !INTERCOM_ACCESS_TOKEN) {
  console.error(
    "Missing env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, INTERCOM_ACCESS_TOKEN",
  );
  process.exit(1);
}

const LIMIT_REPLIES_SCAN = Number(process.env.LIMIT_REPLIES_SCAN ?? "5000");
const INTERCOM_CONCURRENCY = Number(process.env.INTERCOM_CONCURRENCY ?? "2");
const INTERCOM_SLEEP_MS = Number(process.env.INTERCOM_SLEEP_MS ?? "300");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function htmlToText(html) {
  if (!html) return "";
  return String(html)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .trim();
}

function asNumber(v) {
  const n = typeof v === "string" ? Number(v) : v;
  return Number.isFinite(n) ? n : null;
}

function epochToIso(ts) {
  return ts ? new Date(ts * 1000).toISOString() : null;
}

function getEpochSeconds(obj) {
  return (
    asNumber(obj?.created_at) ??
    asNumber(obj?.created_at_unix) ??
    asNumber(obj?.sent_at) ??
    asNumber(obj?.delivered_at) ??
    asNumber(obj?.notified_at) ??
    null
  );
}

function extractTagsFromConversation(conversation) {
  const t1 = conversation?.tags?.tags;
  const t2 = conversation?.tags?.data;
  const t3 = conversation?.tags;

  const arr =
    (Array.isArray(t1) && t1) ||
    (Array.isArray(t2) && t2) ||
    (Array.isArray(t3) && t3) ||
    [];

  return arr.map((t) => t?.name).filter(Boolean).join(", ");
}

function extractUserFromConversation(conversation) {
  const c0 = conversation?.contacts?.contacts?.[0] ?? null;
  const sa = conversation?.source?.author ?? null;

  return {
    user_id: c0?.external_id ?? null,
    user_name: sa?.name ?? null,
    user_email: sa?.email ?? null,
  };
}

function extractInbox(conversation) {
  // Best-effort only (Intercom often omits inbox from this endpoint)
  const inboxId =
    conversation?.inbox?.id ??
    conversation?.source?.inbox?.id ??
    conversation?.inbox_id ??
    null;

  const inboxName =
    conversation?.inbox?.name ??
    conversation?.source?.inbox?.name ??
    conversation?.inbox_name ??
    null;

  return {
    intercom_inbox_id: inboxId != null ? String(inboxId) : null,
    intercom_inbox_name: inboxName != null ? String(inboxName) : null,
  };
}

function collectMessages(conversation) {
  const messages = [];

  if (conversation?.source) messages.push({ ...conversation.source, _from: "source" });

  const parts = conversation?.conversation_parts?.conversation_parts || [];
  for (const p of parts) messages.push({ ...p, _from: "part" });

  messages.sort((a, b) => (getEpochSeconds(a) ?? 0) - (getEpochSeconds(b) ?? 0));
  return messages;
}

/**
 * Only accept real end-user messages as "previous user message":
 * - author.type must be user|lead
 * - must have body
 * - must be:
 *   - conversation.source OR part_type === "comment"
 */
function findPreviousEndUserMessage(messages, i) {
  for (let j = i - 1; j >= 0; j--) {
    const prev = messages[j];
    const authorType = String(prev?.author?.type || "").toLowerCase();
    if (!(authorType === "user" || authorType === "lead")) continue;

    const bodyText = htmlToText(prev?.body || "");
    if (!bodyText.trim()) continue;

    const from = String(prev?._from || "");
    const partType = String(prev?.part_type || "").toLowerCase();

    const isConversationSource = from === "source";
    const isUserMessagePart = partType === "comment";

    if (!isConversationSource && !isUserMessagePart) continue;

    return { text: bodyText, created_at: epochToIso(getEpochSeconds(prev)) };
  }
  return { text: "", created_at: null };
}

async function fetchIntercomConversation(conversationId) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const res = await fetch(`https://api.intercom.io/conversations/${conversationId}`, {
      headers: {
        Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
        Accept: "application/json",
      },
    });

    if (res.ok) return res.json();

    const body = await res.text().catch(() => "");
    const retryable = res.status === 429 || (res.status >= 500 && res.status <= 599);

    if (!retryable || attempt === 5) {
      throw new Error(
        `Intercom fetch failed ${res.status} for ${conversationId}: ${body.slice(0, 500)}`,
      );
    }

    const backoff = 500 * Math.pow(2, attempt);
    await sleep(backoff);
  }

  throw new Error(`Intercom fetch failed for ${conversationId}`);
}

/**
 * Auto-draining selector:
 * Pull a bunch of reply rows that still need backfill, then dedupe to conversation_ids.
 */
async function getConversationIdsNeedingBackfill(limitReplyRows) {
  const { data, error } = await supabase
    .from("replies")
    .select("conversation_id")
    .or(
      "conversation_state.is.null,is_intercom_note.is.null,user_prev_message_created_at.is.null",
    )
    .limit(limitReplyRows);

  if (error) throw new Error(`Failed to query replies: ${error.message}`);

  const ids = (data ?? [])
    .map((r) => (r?.conversation_id != null ? String(r.conversation_id) : null))
    .filter(Boolean);

  return Array.from(new Set(ids));
}

function buildRowsFromConversation(conversation) {
  const conversationId = conversation?.id != null ? String(conversation.id) : null;
  if (!conversationId) return [];

  const tags = extractTagsFromConversation(conversation);
  const { user_id, user_name, user_email } = extractUserFromConversation(conversation);
  const { intercom_inbox_id, intercom_inbox_name } = extractInbox(conversation);

  // Conversation-level fields
  const conversation_state = conversation?.state ?? null;
  const conversation_open = typeof conversation?.open === "boolean" ? conversation.open : null;
  const conversation_waiting_since = epochToIso(asNumber(conversation?.waiting_since));
  const conversation_snoozed_until = epochToIso(asNumber(conversation?.snoozed_until));
  const conversation_updated_at = epochToIso(asNumber(conversation?.updated_at));

  const team_assignee_id =
    conversation?.team_assignee_id != null ? String(conversation.team_assignee_id) : null;
  const assignee_id =
    conversation?.admin_assignee_id != null ? String(conversation.admin_assignee_id) : null;

  const messages = collectMessages(conversation);
  const rows = [];

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    const authorType = String(msg?.author?.type || "").toLowerCase();

    // Only HUMAN teammate replies (exclude bot)
    if (authorType !== "admin") continue;

    const bodyText = htmlToText(msg?.body || "");
    if (!bodyText.trim()) continue;

    const partId = msg?.id ?? msg?.part_id ?? null;
    if (!partId) continue;

    const reply_created_at = epochToIso(getEpochSeconds(msg));
    if (!reply_created_at) continue;

    const { text: user_prev_message, created_at: user_prev_message_created_at } =
      findPreviousEndUserMessage(messages, i);

    const partType = String(msg?.part_type || "").toLowerCase();
    const is_intercom_note = partType === "note";

    rows.push({
      pulled_at: new Date().toISOString(),
      conversation_id: conversationId,
      part_id: String(partId),

      reply_created_at,

      teammate_id: msg?.author?.id != null ? String(msg.author.id) : null,
      teammate_name: msg?.author?.name ?? null,

      tags: tags || null,
      assignee_id,
      team_assignee_id,

      user_id: user_id ?? null,
      user_name: user_name ?? null,
      user_email: user_email ?? null,

      user_prev_message: user_prev_message || null,
      user_prev_message_created_at,

      conversation_state,
      conversation_open,
      conversation_waiting_since,
      conversation_snoozed_until,
      conversation_updated_at,

      // Inbox fields (best-effort)
      inbox: intercom_inbox_id, // legacy column kept in sync
      intercom_inbox_id,
      intercom_inbox_name,

      is_intercom_note,

      agent_reply: bodyText,
    });
  }

  return rows;
}

async function upsertRows(rows) {
  if (!rows.length) return 0;

  const { error } = await supabase.from("replies").upsert(rows, { onConflict: "part_id" });
  if (error) throw new Error(`Supabase upsert failed: ${error.message}`);

  return rows.length;
}

/**
 * Remaining work tracker (rows still missing any of the backfilled fields).
 */
async function getRemainingRowsCount() {
  // We only need an estimate of "still missing", so select count with head:true.
  const { count, error } = await supabase
    .from("replies")
    .select("id", { count: "exact", head: true })
    .or(
      "conversation_state.is.null,is_intercom_note.is.null,user_prev_message_created_at.is.null",
    );

  if (error) throw new Error(`Failed to count remaining rows: ${error.message}`);
  return Number(count ?? 0);
}

function writeGithubOutput(k, v) {
  const outPath = process.env.GITHUB_OUTPUT;
  if (!outPath) return;
  fs.appendFileSync(outPath, `${k}=${String(v)}\n`);
}

async function run() {
  console.log("Backfill run starting", {
    LIMIT_REPLIES_SCAN,
    INTERCOM_CONCURRENCY,
    INTERCOM_SLEEP_MS,
  });

  const conversationIds = await getConversationIdsNeedingBackfill(LIMIT_REPLIES_SCAN);

  if (conversationIds.length === 0) {
    const remaining = await getRemainingRowsCount();
    console.log("No conversations to process in this run.", { remaining_rows: remaining });

    const done = remaining === 0;
    writeGithubOutput("done", done ? "true" : "false");
    writeGithubOutput("remaining_rows", remaining);

    // exit cleanly
    return;
  }

  console.log(`Found ${conversationIds.length} distinct conversation_ids to backfill this run`);

  let processed = 0;
  let rowsUpserted = 0;
  let errors = 0;

  let idx = 0;

  async function worker(workerId) {
    while (true) {
      const my = idx++;
      if (my >= conversationIds.length) break;

      const conversationId = conversationIds[my];

      try {
        const conversation = await fetchIntercomConversation(conversationId);
        const rows = buildRowsFromConversation(conversation);
        const n = await upsertRows(rows);

        rowsUpserted += n;
        processed++;

        if (processed % 25 === 0) {
          console.log(
            `Progress: processed=${processed}/${conversationIds.length} rows_upserted=${rowsUpserted} errors=${errors}`,
          );
        }
      } catch (e) {
        errors++;
        console.error(`Error backfilling conversation ${conversationId}:`, e?.message ?? e);
      }

      await sleep(INTERCOM_SLEEP_MS);
    }

    console.log(`Worker ${workerId} done`);
  }

  const workers = Array.from({ length: INTERCOM_CONCURRENCY }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  const remaining = await getRemainingRowsCount();
  const done = remaining === 0;

  console.log("Backfill run finished", {
    conversations_targeted: conversationIds.length,
    conversations_processed: processed,
    rows_upserted: rowsUpserted,
    errors,
    remaining_rows: remaining,
    done,
  });

  writeGithubOutput("done", done ? "true" : "false");
  writeGithubOutput("remaining_rows", remaining);

  // If you want failures to show up in Actions, keep non-zero exit on errors:
  if (errors > 0) process.exitCode = 1;
}

run().catch((e) => {
  console.error("Fatal:", e?.message ?? e);
  writeGithubOutput("done", "false");
  process.exit(1);
});
