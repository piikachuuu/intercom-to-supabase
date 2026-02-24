import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

// Optional tuning
const BATCH_LIMIT = Number(process.env.BACKFILL_BATCH_LIMIT ?? "5000"); // max rows to fetch from Supabase
const INTERCOM_DELAY_MS = Number(process.env.INTERCOM_DELAY_MS ?? "200"); // ~5 req/sec
const ONLY_TOPIC = process.env.ONLY_TOPIC || ""; // if you store topic/source_type and want to filter later

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !INTERCOM_ACCESS_TOKEN) {
  console.error(
    "Missing env vars. Need SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, INTERCOM_ACCESS_TOKEN"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchConversation(conversationId) {
  const headers = {
    Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
    Accept: "application/json",
  };

  // First page
  const firstUrl = `https://api.intercom.io/conversations/${conversationId}`;
  const res = await fetch(firstUrl, { headers });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(
      `Intercom fetch failed for conversation ${conversationId}: ${res.status} ${res.statusText} ${text}`
    );
  }

  const conversation = await res.json();

  // Collect parts from first page
  const allParts = [
    ...(conversation?.conversation_parts?.conversation_parts || []),
  ];

  // Follow pagination if present
  let nextUri = conversation?.conversation_parts?.pages?.next?.uri || null;

  while (nextUri) {
    // Intercom's next.uri is usually a relative path like "/conversations/123?starting_after=..."
    const url = nextUri.startsWith("http")
      ? nextUri
      : `https://api.intercom.io${nextUri}`;

    const r = await fetch(url, { headers });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      throw new Error(
        `Intercom pagination fetch failed for conversation ${conversationId}: ${r.status} ${r.statusText} ${text}`
      );
    }

    const page = await r.json();

    const pageParts = page?.conversation_parts?.conversation_parts || [];
    allParts.push(...pageParts);

    nextUri = page?.conversation_parts?.pages?.next?.uri || null;

    // be polite to rate limits
    await sleep(INTERCOM_DELAY_MS);
  }

  // Put the hydrated parts back onto the original object
  conversation.conversation_parts = conversation.conversation_parts || {};
  conversation.conversation_parts.conversation_parts = allParts;

  return conversation;
}

function asNumber(v) {
  const n = typeof v === "string" ? Number(v) : v;
  return Number.isFinite(n) ? n : null;
}

function idCandidates(obj) {
  const cands = [
    obj?.id,
    obj?.part_id,
    obj?.uuid,
    obj?.message_id,
    obj?.conversation_part_id,
  ]
    .filter((x) => x !== undefined && x !== null)
    .map(String);

  return [...new Set(cands)];
}

function indexConversationParts(conversation) {
  // Map: id string -> created_at epoch seconds
  const map = new Map();

  const convoFallback =
    asNumber(conversation?.created_at) ??
    asNumber(conversation?.updated_at) ??
    null;

  const add = (obj, fallbackEpoch = null) => {
    const created =
      asNumber(obj?.created_at) ??
      asNumber(obj?.sent_at) ??
      asNumber(obj?.delivered_at) ??
      asNumber(obj?.created_at_unix) ??
      fallbackEpoch ??
      null;

    if (!created) return;

    const ids = [
      obj?.id,
      obj?.part_id,
      obj?.uuid,
      obj?.message_id,
      obj?.conversation_part_id,
    ]
      .filter((x) => x !== undefined && x !== null)
      .map(String);

    for (const id of new Set(ids)) {
      map.set(id, created);
    }
  };

  // ✅ Key fix: add source using conversation.created_at as fallback timestamp
  if (conversation?.source) add(conversation.source, convoFallback);

  const parts = conversation?.conversation_parts?.conversation_parts || [];
  for (const p of parts) add(p, null);

  return map;
}

async function getNullRows(limit) {
  // Adjust selected columns if needed; keep minimal for speed.
  let q = supabase
    .from("replies")
    .select("part_id, conversation_id")
    .is("reply_created_at", null)
    .limit(limit);

  // If you have a column to filter (optional), you can use ONLY_TOPIC.
  // Example:
  // if (ONLY_TOPIC) q = q.eq("topic", ONLY_TOPIC);

  const { data, error } = await q;
  if (error) throw error;
  return data ?? [];
}

async function updateReplyCreatedAt(partId, iso) {
  const { error } = await supabase
    .from("replies")
    .update({ reply_created_at: iso })
    .eq("part_id", String(partId));

  if (error) throw error;
}

async function countRemainingNulls() {
  const { count, error } = await supabase
    .from("replies")
    .select("*", { count: "exact", head: true })
    .is("reply_created_at", null);

  if (error) throw error;
  return count ?? 0;
}

async function main() {
  const rows = await getNullRows(BATCH_LIMIT);
  console.log(`Found ${rows.length} rows with reply_created_at IS NULL (limit=${BATCH_LIMIT})`);

  if (rows.length === 0) {
    console.log("Nothing to backfill. Exiting.");
    return;
  }

  // Group by conversation to minimize Intercom calls
  const byConversation = new Map();
  for (const r of rows) {
    const cid = String(r.conversation_id);
    const pid = String(r.part_id);
    if (!cid || !pid) continue;
    if (!byConversation.has(cid)) byConversation.set(cid, []);
    byConversation.get(cid).push(pid);
  }

  console.log(`Need to fetch ${byConversation.size} unique conversations`);

  let updated = 0;
  let missing = 0;
  let failed = 0;

  for (const [conversationId, partIds] of byConversation.entries()) {
    try {
      const conversation = await fetchConversation(conversationId);
      await sleep(INTERCOM_DELAY_MS);

      const partIndex = indexConversationParts(conversation);
      console.log("has source in index:", partIndex.has(String(conversation?.source?.id)));
      // DEBUG ONE SAMPLE: print IDs we have vs the DB part_id we want
if (updated === 0 && missing === 0) {
  const src = conversation?.source;
  const parts = conversation?.conversation_parts?.conversation_parts || [];

  console.log("==== DEBUG SAMPLE START ====");
  console.log("conversationId:", conversationId);
  console.log("DB partIds (first 10):", partIds.slice(0, 10));

  console.log("source id candidates:", {
    id: src?.id,
    part_id: src?.part_id,
    uuid: src?.uuid,
    message_id: src?.message_id,
    type: src?.type,
  });

  console.log(
    "parts count:",
    parts.length
  );

  console.log(
    "first 10 parts id candidates:",
    parts.slice(0, 10).map((p) => ({
      id: p?.id,
      part_id: p?.part_id,
      uuid: p?.uuid,
      message_id: p?.message_id,
      created_at: p?.created_at,
      part_type: p?.part_type,
      type: p?.type,
    }))
  );

  console.log("==== DEBUG SAMPLE END ====");
}
      for (const partId of partIds) {
        const createdAtEpoch = partIndex.get(String(partId));
        if (!createdAtEpoch) {
          missing++;
          continue;
        }

        const iso = new Date(createdAtEpoch * 1000).toISOString();
        await updateReplyCreatedAt(partId, iso);
        updated++;
      }

      console.log(
        `Conversation ${conversationId}: processed ${partIds.length} (running: updated=${updated}, missing=${missing}, failed=${failed})`
      );
    } catch (e) {
      failed++;
      console.error(`Failed conversation ${conversationId}:`, e?.message ?? e);
    }
  }

  console.log("DONE");
  console.log({ updated, missing, failed, total_null_rows_scanned: rows.length });

  const remaining = await countRemainingNulls();
  console.log(`Remaining rows with reply_created_at IS NULL: ${remaining}`);

  // Optional: fail the job if we couldn't update anything (helps catch config issues)
  if (updated === 0) {
    console.warn("Warning: updated=0. Check Intercom token / table columns / part_id mapping.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
