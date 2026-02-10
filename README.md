# Park Metrics Collector

A simple utility for aggregating theme park wait time data from public sources.

This repo also acts as the **canonical scheduler** for Parkfolio ingestion via GitHub Actions:
- `.github/workflows/collect-parkfolio.yml` calls the Parkfolio Vercel cron endpoint on a schedule.

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

## License

MIT
