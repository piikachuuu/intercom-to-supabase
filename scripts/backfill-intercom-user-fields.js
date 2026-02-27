import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

requireEnv("SUPABASE_URL");
requireEnv("SUPABASE_SERVICE_ROLE_KEY");
requireEnv("INTERCOM_ACCESS_TOKEN");

// Avoid printing secrets. Logging the project URL host is fine.
try {
  const u = new URL(SUPABASE_URL);
  console.log("Supabase host:", u.host);
} catch {
  console.log("Supabase URL provided (could not parse host).");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function compactPatch(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== null && v !== undefined && v !== "") out[k] = v;
  }
  return out;
}

/**
 * Extract a "best effort" user identity from Intercom conversation JSON.
 * Priority:
 * - Email: source.author.email > first part author email > first contact email
 * - Name:  source.author.name  > first part author name  > first contact name
 * - user_id: contact.external_id (Sleeper user id) > fallback intercom ids
 */
function extractUserFromConversation(conversation) {
  const contact0 = conversation?.contacts?.contacts?.[0] ?? null;
  const sourceAuthor = conversation?.source?.author ?? null;

  const parts = conversation?.conversation_parts?.conversation_parts ?? [];
  let partAuthor = null;
  for (const p of parts) {
    const a = p?.author;
    if (a?.email || a?.name || a?.id) {
      partAuthor = a;
      break;
    }
  }

  const user_email =
    sourceAuthor?.email ?? partAuthor?.email ?? contact0?.email ?? null;

  const user_name =
    sourceAuthor?.name ?? partAuthor?.name ?? contact0?.name ?? null;

  const externalId = contact0?.external_id ?? null;
  const fallbackId =
    sourceAuthor?.id ?? partAuthor?.id ?? contact0?.id ?? null;

  const user_id = externalId ?? fallbackId ?? null;

  return { user_id, user_name, user_email };
}

/**
 * Fetch an Intercom conversation.
 * - 404 returns null (skip)
 * - 429 retries with exponential backoff
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

    if (res.status === 404) return null;

    if (res.status === 429) {
      if (attempt > maxRetries) return null;
      const waitMs = Math.min(1000 * 2 ** (attempt - 1), 20000);
      console.log(`Intercom 429. Backing off ${waitMs}ms...`);
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
 * Updates replies for a conversation where any of the user fields are null.
 * Does not overwrite existing non-null values.
 */
async function updateRepliesForConversation(conversationId, user) {
  const patch = compactPatch({
    user_id: user.user_id,
    user_name: user.user_name,
    user_email: user.user_email,
  });

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
async function apiIntrospectionChecks() {
  console.log("Running API introspection checks...");

  const t1 = await supabase.from("replies").select("id").limit(1);
  console.log("select id:", t1.error ? t1.error.message : "ok");

  const t2 = await supabase.from("replies").select("user_email").limit(1);
  console.log("select user_email:", t2.error ? t2.error.message : "ok");

  if (t2.error) {
    console.log("user_email full error:", JSON.stringify(t2.error, null, 2));
  }
}
/**
 * Optional: verify the schema via Supabase API early.
 * If this fails with "column does not exist", we stop immediately with a clear message.
 */
async function verifySupabaseSchema() {
  const { error } = await supabase
    .from("replies")
    .select("user_email")
    .limit(1);

  if (error) {
    // This is the error you're seeing; make it super obvious.
    throw new Error(
      `Supabase API cannot query replies.user_email. This usually means you're pointing at the wrong Supabase project/environment or the migration wasn't applied there.\nOriginal error: ${error.message}`
    );
  }
}

async function main() {
  const BATCH_ROWS = Number(process.env.BATCH_ROWS || 3000);
  const MAX_CONVERSATIONS = Number(process.env.MAX_CONVERSATIONS || 1000);
  const INTERCOM_DELAY_MS = Number(process.env.INTERCOM_DELAY_MS || 150);
  const MAX_LOOPS = Number(process.env.MAX_LOOPS || 999999);

  await verifySupabaseSchema();

  let loops = 0;
  let totalUpdatedRows = 0;
  let skipped404 = 0;
  let processedConversations = 0;

  console.log("Backfill config:", {
    BATCH_ROWS,
    MAX_CONVERSATIONS,
    INTERCOM_DELAY_MS,
  });

  while (loops < MAX_LOOPS) {
    loops += 1;

    const { data: rows, error } = await supabase
      .from("replies")
      .select("conversation_id")
      .or("user_email.is.null,user_name.is.null,user_id.is.null")
      .not("conversation_id", "is", null)
      .limit(BATCH_ROWS);

    if (error) throw error;

    if (!rows?.length) {
      console.log("No more rows missing user fields. Done.");
      break;
    }

    const seen = new Set();
    const conversationIds = [];
    for (const r of rows) {
      const id = String(r.conversation_id);
      if (!seen.has(id)) {
        seen.add(id);
        conversationIds.push(id);
      }
      if (conversationIds.length >= MAX_CONVERSATIONS) break;
    }

    console.log(
      `Loop ${loops}: scanned_rows=${rows.length}, conversations_to_process=${conversationIds.length}`
    );

    for (const conversationId of conversationIds) {
      processedConversations += 1;

      let conversation;
      try {
        conversation = await fetchConversation(conversationId);
      } catch (e) {
        console.error(
          `conversation=${conversationId} intercom_fetch_error=${e?.message ?? e}`
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
          `conversation=${conversationId} updated_rows=${updated} total_updated_rows=${totalUpdatedRows} user_name=${user.user_name ?? "null"} user_email=${user.user_email ?? "null"} user_id=${user.user_id ?? "null"}`
        );
      } catch (e) {
        console.error(
          `conversation=${conversationId} update_error=${e?.message ?? e}`
        );
      }

      await sleep(INTERCOM_DELAY_MS);
    }
  }

  console.log("Backfill finished:", {
    loops,
    processedConversations,
    totalUpdatedRows,
    skipped404,
  });
}

main().catch((e) => {
  console.error(e?.message ?? e);
  process.exit(1);
});
