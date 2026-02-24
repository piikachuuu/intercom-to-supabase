import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function fetchConversationTags(conversationId) {
  const res = await fetch(
    `https://api.intercom.io/conversations/${encodeURIComponent(conversationId)}/tags`,
    {
      headers: {
        Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
        Accept: "application/json",
      },
    }
  );

  if (!res.ok) return "";

  const json = await res.json();
  const tags = Array.isArray(json?.tags) ? json.tags : [];
  return tags.map(t => t?.name).filter(Boolean).join(", ");
}

async function main() {
  const { data: rows } = await supabase
    .from("replies")
    .select("conversation_id")
    .is("tags", null);

  const conversationIds = [...new Set(rows.map(r => r.conversation_id))];

  console.log(`Found ${conversationIds.length} conversations needing tag backfill`);

  for (const id of conversationIds) {
    const tags = await fetchConversationTags(id);

    console.log(`Updating ${id} → tags: ${tags}`);

    await supabase
      .from("replies")
      .update({ tags: tags || null })
      .eq("conversation_id", id);
  }

  console.log("Backfill complete");
}

main();
