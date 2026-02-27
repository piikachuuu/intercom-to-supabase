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

async function fetchConversation(conversationId) {
  const res = await fetch(`https://api.intercom.io/conversations/${conversationId}`, {
    headers: {
      Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
      Accept: "application/json",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Intercom fetch failed ${res.status} for conversation ${conversationId}: ${text}`);
  }
  return res.json();
}

function extractUserFromConversation(conversation) {
  const c0 = conversation?.contacts?.contacts?.[0] ?? null;

  const sa = conversation?.source?.author;
  const sourceUser = sa?.type === "user" ? sa : null;

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

async function main() {
  const BATCH_SIZE = Number(process.env.BATCH_SIZE || 200);
  const MAX_ROWS = Number(process.env.MAX_ROWS || 2000);
  const INTERCOM_DELAY_MS = Number(process.env.INTERCOM_DELAY_MS || 250);

  let processed = 0;
  let updated = 0;

  while (processed < MAX_ROWS) {
    // Pull rows missing user_name (most common). If you want to also backfill when email/external_id
    // is missing but user_name exists, tell me and I’ll adjust the query.
    const { data: rows, error } = await supabase
      .from("replies")
      .select("part_id, conversation_id, user_id, user_name, user_email, user_external_id")
      .is("user_name", null)
      .limit(BATCH_SIZE);

    if (error) throw error;
    if (!rows || rows.length === 0) break;

    // Group by conversation_id to reduce Intercom API calls
    const byConversation = new Map();
    for (const r of rows) {
      if (!r.conversation_id) continue;
      if (!byConversation.has(r.conversation_id)) byConversation.set(r.conversation_id, []);
      byConversation.get(r.conversation_id).push(r);
    }

    for (const [conversationId, convoRows] of byConversation.entries()) {
      let user;
      try {
        const conversation = await fetchConversation(conversationId);
        user = extractUserFromConversation(conversation);
      } catch (e) {
        console.error(String(e));
        processed += convoRows.length;
        continue;
      }

      for (const r of convoRows) {
        processed += 1;

        const patch = {};
        if (r.user_id == null && user.user_id != null) patch.user_id = user.user_id;
        if (r.user_name == null && user.user_name != null) patch.user_name = user.user_name;
        if (r.user_email == null && user.user_email != null) patch.user_email = user.user_email;
        if (r.user_external_id == null && user.user_external_id != null)
          patch.user_external_id = user.user_external_id;

        if (Object.keys(patch).length === 0) continue;

        const { error: updErr } = await supabase
          .from("replies")
          .update(patch)
          .eq("part_id", r.part_id);

        if (updErr) {
          console.error("Update failed for part_id", r.part_id, updErr);
        } else {
          updated += 1;
        }
      }

      await sleep(INTERCOM_DELAY_MS);
      if (processed >= MAX_ROWS) break;
    }
  }

  console.log(`Done. processed=${processed} updated_rows=${updated}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
