# Park Metrics Collector

A simple utility for aggregating theme park wait time data from public sources.

This repo also acts as the **canonical scheduler** for Parkfolio ingestion via GitHub Actions:
- `.github/workflows/collect-parkfolio.yml` calls the Parkfolio Vercel cron endpoint on a schedule.
- `.github/workflows/monitor-parkfolio.yml` validates coverage, freshness, and API contracts every 15 minutes.
- `.github/workflows/prune-supabase-hot-window.yml` enforces a 48-hour Supabase hot window every 30 minutes.

## Purpose

This tool collects publicly available queue time data for research purposes.

## Data Sources

- Queue-Times.com public API
- Open-Meteo weather API

## Setup

1. Clone repository
2. Install dependencies: `npm install`
3. Configure environment variables
4. Run: `npm run collect`

## Environment Variables

```
DB_CONNECTION=your_database_url
DB_AUTH=your_database_key
```

### GitHub Actions Secrets (for `collect-parkfolio.yml`)

- `CRON_SECRET`: must match `CRON_SECRET` configured in the Parkfolio Vercel project
- `BASE_URL` (optional): defaults to `https://parkfolio.vercel.app`

### Additional Secrets (for `monitor-parkfolio.yml` and prune workflow)

- `NEXT_PUBLIC_SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `TURSO_DATABASE_URL` (optional but recommended for direct Turso monitoring)
- `TURSO_AUTH_TOKEN` (optional but recommended for direct Turso monitoring)

### Repository Variables

- `COLLECTOR_SHARD_TOTAL`
  - set to `1` during canary rollout
  - set to `6` for full shard rollout

Secret ownership note:
- `CRON_SECRET` is rotated from `HJSTheJoker/parkfolio` via `.github/workflows/rotate-cron-secret.yml`.
- That workflow updates both repos (`parkfolio` and `park-metrics-collector`) to keep scheduler auth in sync.
- If collector runs start returning `401`, manually trigger `Rotate CRON_SECRET` in `parkfolio`, then run `Parkfolio Collector` once in this repo to verify recovery.

## License

MIT
