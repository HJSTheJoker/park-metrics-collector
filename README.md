# Park Metrics Collector

A simple utility for aggregating theme park wait time data from public sources.

## Purpose

This tool collects publicly available queue time data for research purposes.

## Data Sources

- Queue-Times.com public API
- Open-Meteo weather API

## Setup

1. Clone repository
2. Install dependencies: `npm install` (Node 20.x recommended)
3. Configure environment variables (see below)
4. Run baseline collector: `npm run collect`
5. Run multi-source collector: `npm run enhanced`

## Environment Variables

```
DB_CONNECTION=your_supabase_url
DB_AUTH=your_supabase_service_role_key
TURSO_DATABASE_URL=your_turso_libsql_url
TURSO_AUTH_TOKEN=your_turso_auth_token
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
```

## Scripts

- `npm run collect` - baseline queue/weather collection
- `npm run enhanced` - enhanced collection with ThemeParks mapping and confidence scoring
- `npm run test:apis` - lightweight API reachability test harness

## License

MIT
