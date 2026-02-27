import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;
console.log("SUPABASE_URL:", SUPABASE_URL);
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !INTERCOM_ACCESS_TOKEN) {
  throw new Error(
    "Missing env vars. Need SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, INTERCOM_ACCESS_TOKEN"
  );
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Lead-safe extraction:
 * - Email-only is OK.
 * - user_name can be null.
 * - "external user id" should go into replies.user_id.
 *
 * We try multiple places:
 * - conversation.contacts.contacts[0].external_id (best for your external id)
 * - conversation.source.author.id / parts author id (fallbacks)
 */
function extractUserFromConversation(conversation) {
  const c0 = conversation?.contacts?.contacts?.[0] ?? null;
  const sa = conversation?.source?.author ?? null;

  const parts = conversation?.conversation_parts?.conversation_parts ?? [];
  let partAuthor = null;
  for (const p of parts) {
    const a = p?.author;
    if (a?.email || a?.name || a?.id) {
      partAuthor = a;
      break;
    }
  }

  const bestEmail = sa?.email ?? partAuthor?.email ?? c0?.email ?? null;
  const bestName = sa?.name ?? partAuthor?.name ?? c0?.name ?? null;

  // Your "external user id" (Sleeper user id) should come from Intercom contact external_id
  const externalId = c0?.external_id ?? null;

  // Fallback (not your external id, but better than nothing)
  const fallbackId = sa?.id ?? partAuthor?.id ?? c0?.id ?? null;

  return {
    user_id: externalId ?? fallbackId ?? null,
    user_name: bestName ?? null,
    user_email: bestEmail ?? null,
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
 * Update reply rows for this conversation that are missing ANY of:
 * - user_email
 * - user_name
 * - user_id
 *
 * This supports email-only leads too.
 */
async function updateRepliesForConversation(conversationId, user) {
  const patch = {
    user_id: user.user_id,
    user_name: user.user_name,
    user_email: user.user_email,
  };

  // Do not overwrite with nulls
  for (const k of Object.keys(patch)) {
    if (patch[k] == null) delete patch[k];
  }
  if (Object.keys(patch).length === 0) return 0;

  const { data, error } = await supabase
    .from("replies")
    .update(patch)
    .eq("conversation_id", conversationId)
    .or("user_email.is.null,user_name.is.null,user_id.is.null")
    .select("part_id");

  if (error) throw error;
  return data?.length ?? 0;
}

async function main() {
  const BATCH_ROWS = Number(process.env.BATCH_ROWS || 2000);
  const MAX_CONVERSATIONS = Number(process.env.MAX_CONVERSATIONS || 400);
  const INTERCOM_DELAY_MS = Number(process.env.INTERCOM_DELAY_MS || 150);
  const MAX_LOOPS = Number(process.env.MAX_LOOPS || 999999);

  let loops = 0;
  let totalUpdatedRows = 0;
  let skipped404 = 0;
  let processedConversations = 0;

  console.log("Backfill starting with:");
  console.log({ BATCH_ROWS, MAX_CONVERSATIONS, INTERCOM_DELAY_MS });

  while (loops < MAX_LOOPS) {
    loops += 1;

    const { data: rows, error } = await supabase
      .from("replies")
      .select("conversation_id")
      .or("user_email.is.null,user_name.is.null,user_id.is.null")
      .not("conversation_id", "is", null)
      .limit(BATCH_ROWS);

    if (error) throw error;

    if (!rows || rows.length === 0) {
      console.log("No more rows missing user fields. Done.");
      break;
    }

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

    console.log(
      `Loop ${loops}: scanned_rows=${rows.length}, conversations_to_process=${unique.length}`
    );

    for (const conversationId of unique) {
      processedConversations += 1;

      let conversation;
      try {
        conversation = await fetchConversation(conversationId);
      } catch (e) {
        console.error(
          `conversation=${conversationId} fatal_fetch_error=`,
          e?.message ?? e
        );
        await sleep(INTERCOM_DELAY_MS);
        continue;
      }

      if (!conversation) {
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
          } user_email=${user.user_email ?? "null"} user_id=${user.user_id ?? "null"}`
        );
      } catch (e) {
        console.error(
          `conversation=${conversationId} update_error=`,
          JSON.stringify(e, null, 2)
        );
      }

      await sleep(INTERCOM_DELAY_MS);
    }
  }

  console.log("Backfill finished:");
  console.log({ loops, processedConversations, totalUpdatedRows, skipped404 });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
