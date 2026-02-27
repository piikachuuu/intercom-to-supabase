import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !INTERCOM_ACCESS_TOKEN) {
  throw new Error(
    "Missing env vars. Need SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, INTERCOM_ACCESS_TOKEN"
  );
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractUserFromConversation(conversation) {
  const c0 = conversation?.contacts?.contacts?.[0] ?? null;

  // Best: initial message author if it's a user
  const sa = conversation?.source?.author;
  const sourceUser = sa?.type === "user" ? sa : null;

  // Fallback: first user-authored part
  let firstUserPartAuthor = null;
  const parts = conversation?.conversation_parts?.conversation_parts ?? [];
  for (const p of parts) {
    if ((p?.author?.type || "").toLowerCase() === "user") {
      firstUserPartAuthor = p.author;
      break;
    }
  }

  const best = sourceUser ?? firstUserPartAuthor ?? null;

  return {
    user_id: best?.id ?? c0?.id ?? null,
    user_name: best?.name ?? null,
    user_email: best?.email ?? null,
    user_external_id: c0?.external_id ?? null,
  };
}

/**
 * Fetch a conversation from Intercom.
 * - Returns null on 404 (skip)
 * - Retries 429 with exponential backoff
 */
async function fetchConversation(conversationId, { maxRetries = 5 } = {}) {
  let attempt = 0;

  while (true) {
    attempt += 1;

    const res = await fetch(
      `https://api.intercom.io/conversations/${conversationId}`,
      {
        headers: {
          Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
          Accept: "application/json",
        },
      }
    );

    if (res.status === 404) {
      console.log(`Skipping missing conversation (404): ${conversationId}`);
      return null;
    }

    if (res.status === 429) {
      if (attempt > maxRetries) {
        console.log(`Rate limited too many times; skipping: ${conversationId}`);
        return null;
      }
      // Exponential backoff: 1s, 2s, 4s, 8s, 16s...
      const waitMs = Math.min(1000 * 2 ** (attempt - 1), 20000);
      console.log(`429 rate limit. Waiting ${waitMs}ms then retrying...`);
      await sleep(waitMs);
      continue;
    }

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(
        `Intercom fetch failed ${res.status} for ${conversationId}: ${text}`
      );
    }

    return res.json();
  }
}

/**
 * Update all reply rows for this conversation that are missing fields.
 * Uses "user_name is null" as the gate so it doesn't re-update forever.
 * (If you also want to fill missing email/external_id even when name exists, tell me.)
 */
async function updateRepliesForConversation(conversationId, user) {
  const patch = {
    user_id: user.user_id,
    user_name: user.user_name,
    user_email: user.user_email,
    user_external_id: user.user_external_id,
  };

  // Do not overwrite existing data with nulls
  for (const k of Object.keys(patch)) {
    if (patch[k] == null) delete patch[k];
  }
  if (Object.keys(patch).length === 0) return 0;

  const { data, error } = await supabase
    .from("replies")
    .update(patch)
    .eq("conversation_id", conversationId)
    .is("user_name", null) // only fill missing (idempotent)
    .select("part_id"); // returns rows updated (count via length)

  if (error) throw error;
  return data?.length ?? 0;
}

async function main() {
  // Tune via workflow env
  const BATCH_ROWS = Number(process.env.BATCH_ROWS || 2000); // how many rows to scan per loop
  const MAX_CONVERSATIONS = Number(process.env.MAX_CONVERSATIONS || 400); // convos per run
  const INTERCOM_DELAY_MS = Number(process.env.INTERCOM_DELAY_MS || 150); // delay per convo
  const MAX_LOOPS = Number(process.env.MAX_LOOPS || 999999);

  let loops = 0;
  let totalUpdatedRows = 0;
  let skipped404 = 0;
  let skippedRateLimited = 0;
  let processedConversations = 0;

  console.log("Backfill starting with:");
  console.log({
    BATCH_ROWS,
    MAX_CONVERSATIONS,
    INTERCOM_DELAY_MS,
  });

  while (loops < MAX_LOOPS) {
    loops += 1;

    // Pull some rows that still need backfill
    const { data: rows, error } = await supabase
      .from("replies")
      .select("conversation_id")
      .is("user_name", null)
      .not("conversation_id", "is", null)
      .limit(BATCH_ROWS);

    if (error) throw error;
    if (!rows || rows.length === 0) {
      console.log("No more rows with user_name = null. Done.");
      break;
    }

    // Unique conversations only
    const unique = [];
    const seen = new Set();
    for (const r of rows) {
      const id = String(r.conversation_id);
      if (!seen.has(id)) {
        seen.add(id);
        unique.push(id);
      }
      if (unique.length >= MAX_CONVERSATIONS) break;
    }

    if (unique.length === 0) {
      console.log("No unique conversation ids found in batch. Done.");
      break;
    }

    console.log(
      `Loop ${loops}: scanned_rows=${rows.length}, conversations_to_process=${unique.length}`
    );

    for (const conversationId of unique) {
      processedConversations += 1;

      let conversation;
      try {
        conversation = await fetchConversation(conversationId);
      } catch (e) {
        // non-404/429 errors should be visible (bad token, etc)
        console.error(`conversation=${conversationId} fatal_fetch_error=${String(e)}`);
        // keep going; don't fail whole job for one bad row
        await sleep(INTERCOM_DELAY_MS);
        continue;
      }

      if (!conversation) {
        // Could be 404 or too many 429s
        // We can't distinguish perfectly here because fetchConversation returns null for both.
        // If you want, we can return {conversation:null, reason:"404"} etc.
        skipped404 += 1;
        await sleep(INTERCOM_DELAY_MS);
        continue;
      }

      const user = extractUserFromConversation(conversation);

      try {
        const updated = await updateRepliesForConversation(conversationId, user);
        totalUpdatedRows += updated;

        console.log(
          `conversation=${conversationId} updated_rows=${updated} total_updated_rows=${totalUpdatedRows} user_name=${
            user.user_name ?? "null"
          } external_id=${user.user_external_id ?? "null"}`
        );
      } catch (e) {
        console.error(`conversation=${conversationId} update_error=${String(e)}`);
      }

      await sleep(INTERCOM_DELAY_MS);
    }
  }

  console.log("Backfill finished:");
  console.log({
    loops,
    processedConversations,
    totalUpdatedRows,
    skipped404,
    skippedRateLimited,
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
