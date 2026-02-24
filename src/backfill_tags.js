import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const INTERCOM_ACCESS_TOKEN = process.env.INTERCOM_ACCESS_TOKEN;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

async function fetchConversation(conversationId) {
  const res = await fetch(
    `https://api.intercom.io/conversations/${encodeURIComponent(conversationId)}`,
    {
      headers: {
        Authorization: `Bearer ${INTERCOM_ACCESS_TOKEN}`,
        Accept: "application/json",
      },
    }
  );

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    console.log(
      `Conversation fetch failed convo=${conversationId} status=${res.status} body=${t.slice(0, 200)}`
    );
    return null;
  }

  return res.json();
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

async function main() {
  const pageSize = 1000;
  let from = 0;

  const convos = new Set();

  while (true) {
    const to = from + pageSize - 1;

    const { data, error } = await supabase
      .from("replies")
      .select("conversation_id")
      .is("tags", null)
      .range(from, to);

    if (error) throw error;
    if (!data || data.length === 0) break;

    for (const r of data) {
      if (r.conversation_id) convos.add(String(r.conversation_id));
    }

    console.log(`Scanned rows ${from}-${to}, found convos so far: ${convos.size}`);
    from += pageSize;
  }

  console.log(`Total distinct conversations needing tag fill: ${convos.size}`);

  let updatedConvos = 0;

for (const convoId of convos) {
  const convo = await fetchConversation(convoId);
  if (!convo) continue;

  const tags = extractTagsFromConversation(convo);
  const newValue = tags ? tags : null;

  const { error } = await supabase
    .from("replies")
    .update({ tags: newValue })
    .eq("conversation_id", convoId)
    .is("tags", null);

  if (error) {
    console.log(`Update failed convo=${convoId}: ${error.message}`);
    continue;
  }

  updatedConvos++;
  if (updatedConvos % 25 === 0) {
    console.log(`Updated ${updatedConvos}/${convos.size} conversations...`);
  }
}

console.log("Backfill complete.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
