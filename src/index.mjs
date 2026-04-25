// services/render-ingester/src/index.mjs
//
// Always-on Render web service for tier-3 RSS ingestion.
//
// Mirrors scripts/github-actions/ingest.mjs (RSS parser, HMAC, push, heartbeat)
// but wraps everything in an Express server and self-schedules with
// setInterval(15 min) instead of running once and exiting.

import crypto from "node:crypto";
import express from "express";

// ── Config ───────────────────────────────────────────────────────────────────

const BASE_URL = process.env.CF_PAGES_URL?.replace(/\/$/, "");
const HMAC_SECRET = process.env.INGEST_PUSH_HMAC_SECRET;
const CRON_SECRET = process.env.INTERNAL_CRON_SECRET;
const PORT = Number(process.env.PORT ?? 3000);
const INTERVAL_MS = 15 * 60 * 1000;

function fail(msg) {
  console.error(`[render-ingester] ${msg}`);
  // eslint-disable-next-line no-process-exit
  process.exit(1);
}

if (!BASE_URL) fail("CF_PAGES_URL not set — aborting");
if (!HMAC_SECRET) fail("INGEST_PUSH_HMAC_SECRET not set — aborting");
if (!CRON_SECRET) fail("INTERNAL_CRON_SECRET not set — aborting");

// User-Agent pool — clean browser UAs without custom branding to avoid WAF blocks.
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
];

// ── Module-level state for /health ───────────────────────────────────────────

const state = {
  startedAt: new Date().toISOString(),
  lastRunAt: null,
  lastDurationMs: null,
  lastInserted: 0,
  lastSkipped: 0,
  lastError: null,
  lastStatus: null, // "ok" | "error" | "running"
  runCount: 0,
  running: false,
};

// ── HMAC + helpers ───────────────────────────────────────────────────────────

function signPayload(body) {
  const hmac = crypto.createHmac("sha256", HMAC_SECRET);
  hmac.update(body);
  return `sha256=${hmac.digest("hex")}`;
}

function pickUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function jitter(maxMs) {
  return new Promise((r) => setTimeout(r, Math.floor(Math.random() * maxMs)));
}

// ── Source list ──────────────────────────────────────────────────────────────

async function fetchSources() {
  const url = `${BASE_URL}/api/admin/ingestion/sources?tier=3&platform=render`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${CRON_SECRET}` },
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) {
    throw new Error(`Failed to fetch sources: ${res.status}`);
  }
  const data = await res.json();
  return (data.sources ?? []).filter((s) => s.enabled && s.type === "rss");
}

// ── RSS fetch + parse (lifted from scripts/github-actions/ingest.mjs) ───────

async function fetchRss(url) {
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": pickUserAgent(),
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
    });
    if (!res.ok) {
      console.warn(`[render-ingester] ${res.status} from ${url}`);
      return null;
    }
    return res.text();
  } catch (err) {
    console.warn(`[render-ingester] Failed to fetch ${url}: ${err.message}`);
    return null;
  }
}

function extractTag(xml, tag) {
  const m = xml.match(
    new RegExp(
      `<${tag}(?:[^>]*)><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}(?:[^>]*)>([^<]*)<\\/${tag}>`,
      "i"
    )
  );
  return m ? (m[1] ?? m[2])?.trim() || null : null;
}

function extractAttrTag(xml, tag, attr) {
  const m = xml.match(new RegExp(`<${tag}[^>]+${attr}="([^"]+)"`, "i"));
  return m ? m[1] : null;
}

function safeIsoDate(str) {
  try {
    return new Date(str).toISOString();
  } catch {
    return new Date().toISOString();
  }
}

function parseRssItems(xml, sourceId, category) {
  const items = [];
  const itemPattern =
    /<item[^>]*>([\s\S]*?)<\/item>|<entry[^>]*>([\s\S]*?)<\/entry>/gi;
  let match;
  while ((match = itemPattern.exec(xml)) !== null) {
    const block = match[1] ?? match[2];
    const title = extractTag(block, "title") ?? "";
    const link =
      extractTag(block, "link") ?? extractAttrTag(block, "link", "href") ?? "";
    const description =
      extractTag(block, "description") ?? extractTag(block, "summary") ?? null;
    const pubDate =
      extractTag(block, "pubDate") ??
      extractTag(block, "published") ??
      extractTag(block, "updated") ??
      new Date().toISOString();
    const guid = extractTag(block, "guid") ?? extractTag(block, "id") ?? link;

    if (!title || !link) continue;

    const upperTitle = title.toUpperCase();
    const isBreaking =
      upperTitle.includes("BREAKING") ||
      upperTitle.includes("ALERT") ||
      upperTitle.includes("URGENT") ||
      upperTitle.includes("FLASH")
        ? 1
        : 0;

    items.push({
      id: crypto.randomUUID(),
      sourceId,
      externalId: Buffer.from(guid).toString("base64").slice(0, 64),
      category,
      tags: [],
      headline: title.slice(0, 300),
      summary: description
        ? description.replace(/<[^>]+>/g, "").slice(0, 500)
        : null,
      content: null,
      sourceUrl: link,
      imageUrl: null,
      impact: "LOW",
      publishedAt: safeIsoDate(pubDate),
      fetchedAt: new Date().toISOString(),
      sourceType: "rss",
      isBreaking,
      breakingScore: isBreaking ? 25 : 0,
    });
  }
  return items;
}

// ── Push + heartbeat ─────────────────────────────────────────────────────────

async function pushItems(items) {
  if (items.length === 0) return { inserted: 0, skipped: 0 };
  const body = JSON.stringify(items);
  const signature = signPayload(body);
  const res = await fetch(`${BASE_URL}/api/internal/ingest-push`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Hub-Signature-256": signature,
    },
    body,
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`ingest-push failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function postHeartbeat(itemsIngested, status) {
  const body = JSON.stringify({ platform: "render", itemsIngested, status });
  const sig = signPayload(body);
  try {
    const res = await fetch(`${BASE_URL}/api/internal/ingest-heartbeat`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Hub-Signature-256": sig },
      body,
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) console.warn(`[heartbeat] ${res.status}`);
    else console.log(`[heartbeat] recorded (${status}, ${itemsIngested} items)`);
  } catch (err) {
    console.warn(`[heartbeat] failed: ${err.message}`);
  }
}

// ── Main ingestion run ───────────────────────────────────────────────────────

async function runIngestion() {
  if (state.running) {
    console.warn("[render-ingester] Run already in progress — skipping");
    return { skipped: true };
  }
  state.running = true;
  state.lastStatus = "running";
  const startMs = Date.now();
  console.log("[render-ingester] Starting ingestion run");

  let totalInserted = 0;
  let totalSkipped = 0;
  let anyError = false;
  let lastErrorMsg = null;

  try {
    const sources = await fetchSources();
    console.log(`[render-ingester] ${sources.length} tier-3 RSS sources to ingest`);

    for (const source of sources) {
      // Jitter between sources to spread fingerprint and reduce burst load.
      await jitter(750);
      try {
        const xml = await fetchRss(source.url);
        if (!xml) {
          totalSkipped++;
          continue;
        }
        const items = parseRssItems(xml, source.id, source.category);
        if (items.length === 0) continue;

        for (let i = 0; i < items.length; i += 50) {
          const batch = items.slice(i, i + 50);
          try {
            const result = await pushItems(batch);
            totalInserted += result.inserted ?? 0;
            totalSkipped += result.skipped ?? 0;
          } catch (err) {
            anyError = true;
            lastErrorMsg = err.message;
            totalSkipped += batch.length;
            console.error(
              `[render-ingester] push failed for ${source.name}: ${err.message}`
            );
          }
        }
      } catch (err) {
        anyError = true;
        lastErrorMsg = err.message;
        console.error(
          `[render-ingester] source error for ${source.name}: ${err.message}`
        );
      }
    }
  } catch (err) {
    anyError = true;
    lastErrorMsg = err.message;
    console.error(`[render-ingester] Fatal run error: ${err.message}`);
  }

  const status = anyError ? "error" : "ok";
  await postHeartbeat(totalInserted, status);

  const durationMs = Date.now() - startMs;
  state.lastRunAt = new Date().toISOString();
  state.lastDurationMs = durationMs;
  state.lastInserted = totalInserted;
  state.lastSkipped = totalSkipped;
  state.lastError = lastErrorMsg;
  state.lastStatus = status;
  state.runCount++;
  state.running = false;

  console.log(
    `[render-ingester] Run done in ${durationMs}ms — ${totalInserted} inserted, ${totalSkipped} skipped, status=${status}`
  );
  return { inserted: totalInserted, skipped: totalSkipped, durationMs, status };
}

// ── HTTP server ──────────────────────────────────────────────────────────────

const app = express();
app.disable("x-powered-by");

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    startedAt: state.startedAt,
    uptimeSec: Math.round((Date.now() - Date.parse(state.startedAt)) / 1000),
    runCount: state.runCount,
    running: state.running,
    lastRunAt: state.lastRunAt,
    lastStatus: state.lastStatus,
    lastDurationMs: state.lastDurationMs,
    lastInserted: state.lastInserted,
    lastSkipped: state.lastSkipped,
    lastError: state.lastError,
  });
});

app.post("/run", express.json(), (req, res) => {
  const auth = req.headers.authorization ?? "";
  if (auth !== `Bearer ${CRON_SECRET}`) {
    return res.status(401).json({ error: "unauthorized" });
  }
  if (state.running) {
    return res.status(409).json({ error: "run already in progress" });
  }
  // Fire-and-forget — don't block the HTTP response on the long ingestion.
  runIngestion().catch((err) =>
    console.error("[render-ingester] /run failed:", err)
  );
  return res.json({ ok: true, started: true });
});

// Full single-source debug: fetch → parse → push → report (auth required)
// Scan all assigned sources (no jitter, no push) and report which ones return XML vs null
app.get("/debug-run", async (req, res) => {
  const auth = req.headers.authorization ?? "";
  if (auth !== `Bearer ${CRON_SECRET}`) return res.status(401).json({ error: "unauthorized" });
  const summary = { total: 0, ok: 0, null: 0, sources: [] };
  try {
    const sources = await fetchSources();
    summary.total = sources.length;
    for (const source of sources) {
      try {
        const xml = await fetchRss(source.url);
        const result = { id: source.id, url: source.url.slice(0, 80), ok: !!xml, len: xml?.length ?? 0 };
        if (xml) summary.ok++;
        else summary.null++;
        summary.sources.push(result);
      } catch (err) {
        summary.sources.push({ id: source.id, url: source.url.slice(0, 80), ok: false, error: err.message });
        summary.null++;
      }
    }
  } catch (err) {
    return res.json({ error: err.message });
  }
  res.json(summary);
});

app.get("/debug-source", async (req, res) => {
  const auth = req.headers.authorization ?? "";
  if (auth !== `Bearer ${CRON_SECRET}`) return res.status(401).json({ error: "unauthorized" });
  const url = req.query.url;
  const sourceId = req.query.sourceId ?? "debug";
  const category = req.query.category ?? "commodities";
  if (!url) return res.status(400).json({ error: "?url= required" });

  const result = { url, fetchStatus: null, bodyLength: 0, itemsParsed: 0, pushResult: null, error: null };
  try {
    const r = await fetchRss(url);
    if (!r) {
      result.fetchStatus = "null (fetch returned null)";
    } else {
      result.bodyLength = r.length;
      const items = parseRssItems(r, sourceId, category);
      result.itemsParsed = items.length;
      if (items.length > 0) {
        result.pushResult = await pushItems(items.slice(0, 5));
      }
      result.fetchStatus = "ok";
    }
  } catch (err) {
    result.error = err.message;
  }
  res.json(result);
});

app.get("/test-fetch", express.urlencoded({ extended: false }), async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({ error: "?url= required" });
  try {
    const r = await fetch(url, {
      headers: {
        "User-Agent": pickUserAgent(),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(10000),
    });
    const body = await r.text();
    res.json({ status: r.status, ok: r.ok, bodyLength: body.length, bodyPreview: body.slice(0, 200) });
  } catch (err) {
    res.json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`[render-ingester] Listening on :${PORT}`);
  // First run on startup, then every 15 min.
  runIngestion().catch((err) =>
    console.error("[render-ingester] startup run failed:", err)
  );
  setInterval(() => {
    runIngestion().catch((err) =>
      console.error("[render-ingester] scheduled run failed:", err)
    );
  }, INTERVAL_MS);
});
