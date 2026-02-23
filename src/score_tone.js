import { createClient } from "@supabase/supabase-js";

// ===== CONFIG (mirrors your Apps Script) =====
const OPENAI_MODEL = "gpt-4o-mini";
const MAX_OUTPUT_TOKENS = 400;
const TEMPERATURE = 0.2;

// Run controls
const MAX_TO_SCORE_PER_RUN = 50;
const HARD_DEADLINE_MS = 4 * 60 * 1000;

// Budget guardrails
const MONTHLY_BUDGET_USD = 200.0;
const BUDGET_BUFFER_USD = 15.0;

// Conservative pricing estimates (USD per 1M tokens)
const INPUT_USD_PER_1M = 0.15;
const OUTPUT_USD_PER_1M = 0.6;

// Retry / pacing
const BASE_SLEEP_MS = 120;
const RETRY_MAX = 3;
const RETRY_BASE_MS = 800;
const OPENAI_FETCH_TIMEOUT_MS = 30000;

// Backlog mode
const BACKLOG_MODE_DEFAULT = true; // matches your script default

// State keys
const TONE_BACKLOG_MODE_KEY = "tone_backlog_mode";                 // "true"/"false"
const TONE_BACKLOG_BOUNDARY_TS_KEY = "tone_backlog_boundary_ts";   // ISO timestamp
const TONE_CURSOR_TS_KEY = "tone_cursor_reply_created_at";         // ISO timestamp
const SPENT_MONTH_KEY = "tone_spent_month_key";
const SPENT_USD_KEY = "tone_spent_usd_current_month";

// ===== ENV =====
function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const SUPABASE_URL = requireEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = requireEnv("SUPABASE_SERVICE_ROLE_KEY");
const OPENAI_API_KEY = requireEnv("OPENAI_API_KEY");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ===== State helpers =====
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

// ===== Budget helpers (same logic) =====
function getMonthKeyUTC() {
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${yyyy}-${mm}`;
}

async function getSpendState() {
  const monthKey = getMonthKeyUTC();
  const storedMonth = await getState(SPENT_MONTH_KEY);

  if (storedMonth !== monthKey) {
    await setState(SPENT_MONTH_KEY, monthKey);
    await setState(SPENT_USD_KEY, "0");
  }

  const spent = Number((await getState(SPENT_USD_KEY)) || "0");
  return { monthKey, spent };
}

async function addSpend(usd) {
  const { spent } = await getSpendState();
  await setState(SPENT_USD_KEY, String(spent + usd));
}

function estimateTokensFromText(text) {
  if (!text) return 0;
  return Math.ceil(text.length / 4);
}

function estimateCostUsd(inputText, maxOutputTokens) {
  const inputTokens = estimateTokensFromText(inputText);
  const outputTokens = maxOutputTokens;
  const cost =
    (inputTokens / 1_000_000) * INPUT_USD_PER_1M +
    (outputTokens / 1_000_000) * OUTPUT_USD_PER_1M;
  return cost * 1.25; // same 25% safety buffer
}

// ===== Filters (same) =====
function isInternalNote(text) {
  if (!text) return false;
  const t = String(text).toLowerCase().replace(/\s+/g, " ").trim();

  if (t.includes("is blocked because")) return true;

  if (
    t.includes("we are currently reviewing your submitted information") &&
    t.includes("will have a team member get back to you within an hour")
  ) {
    return true;
  }
  return false;
}

// ===== OpenAI prompt + schema =====
function toneInstructions() {
  return (
    "You are a customer support QA reviewer.\n\n" +
    "Evaluate the AGENT REPLY in the context of the PREVIOUS USER MESSAGE.\n" +
    "Your goal is to assess tone, clarity, ownership, and professionalism from a customer experience perspective.\n\n" +
    "General guidelines:\n" +
    "- Judge the reply fairly and consistently.\n" +
    "- Assume good intent unless the language suggests otherwise.\n" +
    "- If the user is upset or hostile, do NOT penalize professionalism if the agent remains calm.\n" +
    "- Do NOT penalize agents for enforcing rules, compliance steps, or redirecting to third parties when appropriate.\n\n" +
    "Product context:\n" +
    "This is a real-money gaming and wallet product with strict compliance, fraud, and payment rules.\n" +
    "Agents may need to require identity verification, payment provider contact, or offline processes.\n" +
    "These actions can still score highly when explained clearly and respectfully.\n\n" +
    "Scoring rubric (use these definitions exactly):\n\n" +
    "Empathy:\n" +
    "5 — Clearly acknowledges the user’s feelings and validates their experience; warm, human tone.\n" +
    "4 — Friendly and supportive with some personalization.\n" +
    "3 — Neutral and transactional; not rude, but not warm.\n" +
    "2 — Cold or abrupt; little acknowledgement of the user’s feelings.\n" +
    "1 — Dismissive, blaming, or hostile; user likely feels disrespected.\n\n" +
    "Clarity:\n" +
    "5 — Very easy to understand; clear, actionable steps; anticipates confusion.\n" +
    "4 — Clear guidance with minor ambiguity.\n" +
    "3 — Understandable but may require follow-up.\n" +
    "2 — Confusing or incomplete; likely to create more questions.\n" +
    "1 — Incorrect, contradictory, or unusable guidance.\n\n" +
    "Ownership:\n" +
    "5 — Takes responsibility and actively drives resolution; sets expectations and timelines.\n" +
    "4 — Offers help and next steps; generally proactive.\n" +
    "3 — Helps but mostly reactive; limited commitment.\n" +
    "2 — Deflects responsibility or pushes work onto the user.\n" +
    "1 — Refuses help or provides a dead end with no path forward.\n\n" +
    "Professionalism:\n" +
    "5 — Respectful, calm, brand-aligned; handles conflict well.\n" +
    "4 — Professional and friendly with minor tone issues.\n" +
    "3 — Generally fine but slightly blunt or overly casual.\n" +
    "2 — Snippy, defensive, or overly firm; risks escalation.\n" +
    "1 — Rude, sarcastic, or unprofessional language.\n\n" +
    "Primary tone category (choose exactly one):\n" +
    "- friendly\n- neutral\n- apologetic\n- firm\n- confident\n- cold\n- unknown\n\n" +
    "Risk flags:\n" +
    "- rude\n- dismissive\n- blaming_user\n- policy_risk\n- privacy_risk\n- unclear\n- none\n\n" +
    "Output rules:\n" +
    "- Return ONLY a JSON object that strictly matches the provided schema.\n" +
    "- Do not include explanations, commentary, or extra text."
  );
}

function toneSchema() {
  return {
    type: "object",
    additionalProperties: false,
    required: [
      "empathy", "clarity", "ownership", "professionalism",
      "primary_tone", "risk_flags", "confidence",
      "coaching_tip", "rewrite_suggestion"
    ],
    properties: {
      empathy: { type: "integer", minimum: 1, maximum: 5 },
      clarity: { type: "integer", minimum: 1, maximum: 5 },
      ownership: { type: "integer", minimum: 1, maximum: 5 },
      professionalism: { type: "integer", minimum: 1, maximum: 5 },
      primary_tone: { type: "string", enum: ["friendly","neutral","apologetic","firm","confident","cold","unknown"] },
      risk_flags: {
        type: "array",
        items: { type: "string", enum: ["rude","dismissive","blaming_user","policy_risk","privacy_risk","unclear","none"] }
      },
      confidence: { type: "number", minimum: 0, maximum: 1 },
      coaching_tip: { type: "string", maxLength: 280 },
      rewrite_suggestion: { type: "string", maxLength: 700 }
    }
  };
}

// ===== OpenAI call + retries =====
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normalizeScore(obj) {
  const out = { ...obj };
  if (!Array.isArray(out.risk_flags) || out.risk_flags.length === 0) out.risk_flags = ["none"];
  if (out.risk_flags.includes("none") && out.risk_flags.length > 1) {
    out.risk_flags = out.risk_flags.filter((x) => x !== "none");
  }
  if (typeof out.confidence !== "number") out.confidence = 0.5;
  return out;
}

function extractJsonFromResponses(respJson) {
  if (typeof respJson.output_text === "string" && respJson.output_text.trim()) {
    return JSON.parse(respJson.output_text);
  }

  const output = respJson.output || [];
  for (const item of output) {
    if (item && item.type === "message") {
      const content = item.content || [];
      for (const c of content) {
        if (c && typeof c.text === "string") return JSON.parse(c.text);
        if (c && typeof c.output_text === "string") return JSON.parse(c.output_text);
      }
    }
  }
  throw new Error("Could not extract structured JSON from OpenAI response.");
}

async function scoreOneReply(apiKey, inputText) {
  const requestBody = {
    model: OPENAI_MODEL,
    instructions: toneInstructions(),
    input: [{ role: "user", content: inputText }],
    text: {
      format: {
        type: "json_schema",
        name: "tone_score",
        strict: true,
        schema: toneSchema(),
      },
    },
    max_output_tokens: MAX_OUTPUT_TOKENS,
    temperature: TEMPERATURE,
  };

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), OPENAI_FETCH_TIMEOUT_MS);

  let res, bodyText;
  try {
    res = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });
    bodyText = await res.text();
  } finally {
    clearTimeout(t);
  }

  if (!res.ok) {
    throw new Error(`OpenAI error ${res.status}: ${String(bodyText || "").slice(0, 2000)}`);
  }

  const json = JSON.parse(bodyText);
  const parsed = extractJsonFromResponses(json);
  return normalizeScore(parsed);
}

async function scoreOneReplyWithRetry(apiKey, inputText) {
  let lastErr = null;

  for (let attempt = 0; attempt <= RETRY_MAX; attempt++) {
    try {
      return await scoreOneReply(apiKey, inputText);
    } catch (e) {
      lastErr = e;
      const msg = e?.message ? String(e.message) : String(e);

      const retryable =
        msg.includes("OpenAI error 429") ||
        msg.includes("OpenAI error 500") ||
        msg.includes("OpenAI error 502") ||
        msg.includes("OpenAI error 503") ||
        msg.includes("OpenAI error 504") ||
        msg.toLowerCase().includes("aborted") ||
        msg.toLowerCase().includes("timed out");

      if (!retryable || attempt === RETRY_MAX) throw e;

      const backoff = RETRY_BASE_MS * Math.pow(2, attempt);
      console.log(`Retrying OpenAI (attempt ${attempt + 1}/${RETRY_MAX}) after ${backoff}ms: ${msg}`);
      await sleep(backoff);
    }
  }

  throw lastErr || new Error("Unknown error in scoreOneReplyWithRetry");
}

// ===== Backlog boundary + cursor logic (DB version of your sheet logic) =====
async function ensureBacklogBoundaryIfMissing() {
  // Boundary = snapshot of "now" the first time you run backlog mode,
  // so you process up to that point, then switch to live.
  const boundary = await getState(TONE_BACKLOG_BOUNDARY_TS_KEY);
  if (boundary) return boundary;

  const nowIso = new Date().toISOString();
  await setState(TONE_BACKLOG_BOUNDARY_TS_KEY, nowIso);
  return nowIso;
}

async function isBacklogEnabled() {
  const v = await getState(TONE_BACKLOG_MODE_KEY);
  if (v === "") return BACKLOG_MODE_DEFAULT;
  return String(v).toLowerCase() === "true";
}

async function setBacklogEnabled(boolVal) {
  await setState(TONE_BACKLOG_MODE_KEY, boolVal ? "true" : "false");
}

async function getCursorTs() {
  // cursor is reply_created_at ISO; if empty, start from earliest time
  const v = await getState(TONE_CURSOR_TS_KEY);
  return v || "";
}

async function setCursorTs(iso) {
  await setState(TONE_CURSOR_TS_KEY, iso);
}

// Pull next batch of unscored agent replies after cursor_ts,
// optionally capped by backlog boundary.
async function fetchBatch({ cursorTs, boundaryTs, limit }) {
  // Strategy: fetch candidate replies after cursor, then anti-join in JS against tone_scores.
  // (Later we can optimize with an RPC for pure SQL anti-join.)
  let q = supabase
    .from("replies")
    .select("part_id, conversation_id, teammate_name, user_prev_message, agent_reply, reply_created_at")
    .order("reply_created_at", { ascending: true })
    .limit(500); // window to find unscored

  if (cursorTs) q = q.gt("reply_created_at", cursorTs);
  if (boundaryTs) q = q.lte("reply_created_at", boundaryTs);

  const { data: candidates, error } = await q;
  if (error) throw error;

  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.part_id).filter(Boolean);
  if (!ids.length) return [];

  const { data: scored, error: scoredErr } = await supabase
    .from("tone_scores")
    .select("part_id")
    .in("part_id", ids);

  if (scoredErr) throw scoredErr;

  const scoredSet = new Set((scored || []).map((s) => s.part_id));
  const unscored = candidates.filter((r) => !scoredSet.has(r.part_id));

  // Filter internal notes + empty
  const filtered = unscored.filter((r) => {
    const reply = String(r.agent_reply || "");
    if (!reply.trim()) return false;
    if (isInternalNote(reply)) return false;
    return true;
  });

  return filtered.slice(0, limit);
}

async function insertError(partId, conversationId, msg) {
  await supabase.from("tone_score_errors").insert({
    errored_at: new Date().toISOString(),
    part_id: partId || null,
    conversation_id: conversationId || null,
    error_message: String(msg || "").slice(0, 1000),
  });
}

// ===== Main job =====
async function main() {
  const startedMs = Date.now();
  const deadlineAt = startedMs + HARD_DEADLINE_MS;

  const spendState = await getSpendState();
  console.log(`Spend this month (${spendState.monthKey}): $${spendState.spent.toFixed(4)} / $${MONTHLY_BUDGET_USD.toFixed(2)}`);

  if (spendState.spent >= MONTHLY_BUDGET_USD - BUDGET_BUFFER_USD) {
    console.log(`Budget cap reached; exiting.`);
    return;
  }

  const backlogEnabled = await isBacklogEnabled();
  const boundaryTs = backlogEnabled ? await ensureBacklogBoundaryIfMissing() : "";

  let cursorTs = await getCursorTs(); // ISO timestamp
  console.log(`BacklogEnabled=${backlogEnabled} cursorTs=${cursorTs || "(none)"} boundaryTs=${boundaryTs || "(none)"}`);

  const batch = await fetchBatch({
    cursorTs,
    boundaryTs,
    limit: MAX_TO_SCORE_PER_RUN,
  });

  if (!batch.length) {
    console.log("No unscored replies found in window.");

    // Backlog auto-switch: if backlog enabled and nothing left up to boundary, turn it off.
    if (backlogEnabled) {
      console.log("Backlog appears complete up to boundary; switching backlog mode OFF.");
      await setBacklogEnabled(false);
    }

    return;
  }

  let scoredCount = 0;
  let lastProcessedTs = cursorTs;

  for (const r of batch) {
    if (Date.now() > deadlineAt) {
      console.log("Deadline reached; stopping early.");
      break;
    }

    const inputText =
      "PREVIOUS USER MESSAGE:\n" + (r.user_prev_message || "(none)") + "\n\n" +
      "AGENT REPLY:\n" + (r.agent_reply || "");

    const estCost = estimateCostUsd(inputText, MAX_OUTPUT_TOKENS);
    const spendNow = await getSpendState();
    if (spendNow.spent + estCost >= MONTHLY_BUDGET_USD - BUDGET_BUFFER_USD) {
      console.log(`Stopping: would exceed budget. estCost=$${estCost.toFixed(4)} spent=$${spendNow.spent.toFixed(4)}`);
      break;
    }

    try {
      console.log(`Scoring part_id=${r.part_id} reply_created_at=${r.reply_created_at}...`);
      const score = await scoreOneReplyWithRetry(OPENAI_API_KEY, inputText);

      const row = {
        scored_at: new Date().toISOString(),
        part_id: r.part_id,
        conversation_id: r.conversation_id,
        teammate_name: r.teammate_name,

        empathy: score.empathy,
        clarity: score.clarity,
        ownership: score.ownership,
        professionalism: score.professionalism,

        primary_tone: score.primary_tone,
        risk_flags: (score.risk_flags || []).join(", "),
        confidence: score.confidence,

        coaching_tip: score.coaching_tip,
        rewrite_suggestion: score.rewrite_suggestion,

        model: OPENAI_MODEL,
      };

      const { error } = await supabase
        .from("tone_scores")
        .upsert(row, { onConflict: "part_id" });

      if (error) throw error;

      await addSpend(estCost);

      scoredCount++;
      lastProcessedTs = r.reply_created_at || lastProcessedTs;

      await sleep(BASE_SLEEP_MS);
    } catch (e) {
      const msg = e?.message ? String(e.message) : String(e);
      console.log(`Failed scoring part_id=${r.part_id}: ${msg}`);
      await insertError(r.part_id, r.conversation_id, msg);
      await sleep(BASE_SLEEP_MS);
      // cursor still moves forward via lastProcessedTs to avoid getting stuck
      lastProcessedTs = r.reply_created_at || lastProcessedTs;
    }
  }

  // advance cursor to the latest processed timestamp
  if (lastProcessedTs) {
    await setCursorTs(lastProcessedTs);
  }

  // backlog auto-switch: if we reached/passed boundary, disable
  if (backlogEnabled && boundaryTs && lastProcessedTs && new Date(lastProcessedTs) >= new Date(boundaryTs)) {
    console.log(`Reached backlog boundary (${boundaryTs}); switching backlog mode OFF.`);
    await setBacklogEnabled(false);
  }

  const finalSpend = await getSpendState();
  const runtimeS = (Date.now() - startedMs) / 1000;
  console.log(`Done. Scored=${scoredCount} cursorTs=${await getCursorTs()} BacklogEnabled=${await isBacklogEnabled()} Spend=$${finalSpend.spent.toFixed(4)} Runtime=${runtimeS.toFixed(1)}s`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
