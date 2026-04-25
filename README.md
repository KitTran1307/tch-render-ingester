# tch-render-ingester

Always-on Render web service that handles tier-3 ingestion (~37 sources at 15-min cadence) for TwoCentsHustler. Offloads CPU-heavy RSS fan-out from Cloudflare Workers.

## What it does

- Self-schedules with `setInterval` (15 minutes); also runs immediately on startup.
- Fetches `tier=3, platform=render` sources from `${CF_PAGES_URL}/api/admin/ingestion/sources`.
- Parses RSS items (no external deps), HMAC-signs batches, POSTs to `/api/internal/ingest-push`.
- POSTs a heartbeat to `/api/internal/ingest-heartbeat` after each run with `platform: "render"`.
- Exposes `GET /health` (status + uptime + last run state) for Render health checks.
- Exposes `POST /run` (bearer-auth via `INTERNAL_CRON_SECRET`) for manual triggers.

If the CF watchdog detects a stale Render heartbeat (>30 min), Cloudflare picks up render-affinity sources automatically.

## Deploy

1. Connect the GitHub repo to Render (https://render.com).
2. New → Web Service → use `services/render-ingester/render.yaml` (Render auto-detects).
3. Set the three env vars (Render dashboard, "Environment" tab):
   - `CF_PAGES_URL` — e.g. `https://twocentshustler.me`
   - `INGEST_PUSH_HMAC_SECRET` — must match the CF Pages binding
   - `INTERNAL_CRON_SECRET` — must match the CF Pages binding
4. Deploy. The first heartbeat appears in `/admin/ingestion` within ~16 minutes.

## Local smoke test

```bash
cd services/render-ingester
npm install
CF_PAGES_URL=https://twocentshustler.me \
INGEST_PUSH_HMAC_SECRET=... \
INTERNAL_CRON_SECRET=... \
npm start
```

The server starts on the `PORT` env (Render injects automatically; defaults to `3000` locally).

## Free-tier notes

- Render free tier: 750 service-hours/month — enough for one always-on small service.
- This service must be the only one on the free plan, otherwise it'll exceed quota.
- Render free services spin down after ~15 min of HTTP inactivity, but `setInterval` keeps the process active so it stays up.
