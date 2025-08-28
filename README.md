# Park Metrics Collector

A simple utility for aggregating theme park wait time data from public sources.

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

## License

MIT