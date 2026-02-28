#!/usr/bin/env tsx

import * as fs from 'fs'
import { createClient as createTursoClient } from '@libsql/client'
import { createClient } from '@supabase/supabase-js'

type CheckStatus = 'pass' | 'warn' | 'fail'

type Check = {
  name: string
  status: CheckStatus
  detail: string
}

type CronLogRow = {
  status: string
  created_at: string
  details: Record<string, any> | null
}

function env(name: string): string | undefined {
  const value = process.env[name]
  return value && value.trim().length > 0 ? value.trim() : undefined
}

function requiredEnv(name: string): string {
  const value = env(name)
  if (!value) throw new Error(`Missing required env var: ${name}`)
  return value
}

function optionalEnvAny(names: string[]): string | undefined {
  for (const name of names) {
    const value = env(name)
    if (value) return value
  }
  return undefined
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const raw = env(name)
  const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, parsed))
}

function toNumber(value: unknown): number | null {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return Number.POSITIVE_INFINITY
  const sorted = [...values].sort((a, b) => a - b)
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))
  return sorted[idx]
}

function formatStatus(status: CheckStatus) {
  if (status === 'pass') return 'PASS'
  if (status === 'warn') return 'WARN'
  return 'FAIL'
}

function hasMetaFields(meta: Record<string, any> | undefined, fields: string[]): string[] {
  const missing: string[] = []
  for (const field of fields) {
    if (!(field in (meta ?? {}))) missing.push(field)
  }
  return missing
}

async function fetchJson(url: string) {
  const response = await fetch(url)
  const text = await response.text()
  let json: any = null
  try {
    json = text ? JSON.parse(text) : null
  } catch {
    json = null
  }
  return { ok: response.ok, status: response.status, json, text }
}

async function main() {
  const startedAt = new Date()
  const checks: Check[] = []

  const supabaseUrl = requiredEnv('NEXT_PUBLIC_SUPABASE_URL')
  const supabaseServiceRoleKey = requiredEnv('SUPABASE_SERVICE_ROLE_KEY')
  const baseUrl = (env('BASE_URL') ?? 'https://parkfolio.vercel.app').replace(/\/$/, '')
  const cronSecret = env('CRON_SECRET')

  const lookbackMinutes = envInt('MONITOR_LOOKBACK_MINUTES', 30, 5, 240)
  const expectedTickMinutes = envInt('EXPECTED_TICK_MINUTES', 5, 1, 60)
  const expectedShardTotal = envInt('EXPECTED_SHARD_TOTAL', 6, 1, 64)
  const maxFreshnessMinutes = envInt('MAX_P95_FRESHNESS_MINUTES', 15, 5, 240)
  const retentionHours = envInt('SUPABASE_RETENTION_HOURS', 48, 1, 24 * 30)
  const reportFile = env('REPORT_FILE') ?? 'collector-monitor-report.md'
  const tursoUrl = optionalEnvAny(['TURSO_DATABASE_URL', 'TURSO_DB_URL'])
  const tursoToken = optionalEnvAny(['TURSO_AUTH_TOKEN', 'TURSO_TOKEN'])

  const sinceIso = new Date(Date.now() - lookbackMinutes * 60 * 1000).toISOString()
  const retentionCutoffIso = new Date(Date.now() - retentionHours * 60 * 60 * 1000).toISOString()

  const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, {
    auth: { persistSession: false },
  })

  const summary: Record<string, any> = {
    startedAt: startedAt.toISOString(),
    baseUrl,
    lookbackMinutes,
    expectedTickMinutes,
    expectedShardTotal,
    maxFreshnessMinutes,
    retentionHours,
    retentionCutoffIso,
    sinceIso,
  }

  // 1) Coverage + timeout trend from cron_logs
  const { data: cronRows, error: cronError } = await supabase
    .from('cron_logs')
    .select('status, created_at, details')
    .eq('job_name', 'collect-queue-times')
    .gte('created_at', sinceIso)
    .order('created_at', { ascending: true })

  if (cronError) {
    checks.push({
      name: 'Cron coverage window',
      status: 'fail',
      detail: `Failed to query cron_logs: ${cronError.message}`,
    })
  } else {
    const logs = (cronRows ?? []) as CronLogRow[]
    const expectedTicks = Math.max(1, Math.floor(lookbackMinutes / expectedTickMinutes))
    const expectedRuns = expectedTicks * expectedShardTotal

    const runKeysAll = new Set<string>()
    const runKeysCompleted = new Set<string>()
    let timedOutParks = 0
    let parksTargeted = 0
    let failedRuns = 0

    for (const row of logs) {
      const tickBucket = String(row.details?.tickBucket ?? row.details?.tick_bucket ?? '')
      const shardIndex = toNumber(row.details?.shardIndex ?? row.details?.shard_index)
      const shardTotal = toNumber(row.details?.shardTotal ?? row.details?.shard_total)

      if (tickBucket && shardIndex != null && shardTotal != null) {
        const key = `${tickBucket}|${shardIndex}|${shardTotal}`
        runKeysAll.add(key)
        if (row.status === 'completed') {
          runKeysCompleted.add(key)
        }
      }

      const targeted = toNumber(row.details?.parksTargeted)
      const timedOut = toNumber(row.details?.timedOutParks)
      if (targeted != null) parksTargeted += targeted
      if (timedOut != null) timedOutParks += timedOut
      if (row.status === 'failed') failedRuns += 1
    }

    const observedRuns = runKeysCompleted.size
    const observedAnyRuns = runKeysAll.size
    const coverageRatio = expectedRuns > 0 ? observedRuns / expectedRuns : 0
    const timeoutRate = parksTargeted > 0 ? timedOutParks / parksTargeted : 0
    const failedRunRatio = logs.length > 0 ? failedRuns / logs.length : 0

    summary.coverage = {
      expectedRuns,
      observedRuns,
      observedAnyRuns,
      coverageRatio,
      parksTargeted,
      timedOutParks,
      timeoutRate,
      failedRuns,
      failedRunRatio,
    }

    if (coverageRatio < 0.8) {
      checks.push({
        name: 'Cron coverage window',
        status: 'fail',
        detail: `Observed ${observedRuns}/${expectedRuns} runs (${(coverageRatio * 100).toFixed(1)}%).`,
      })
    } else if (coverageRatio < 0.95) {
      checks.push({
        name: 'Cron coverage window',
        status: 'warn',
        detail: `Coverage is degraded: ${observedRuns}/${expectedRuns} runs (${(coverageRatio * 100).toFixed(1)}%).`,
      })
    } else {
      checks.push({
        name: 'Cron coverage window',
        status: 'pass',
        detail: `Coverage healthy: completed ${observedRuns}/${expectedRuns} runs (${(coverageRatio * 100).toFixed(1)}%), observed any-status ${observedAnyRuns}.`,
      })
    }

    if (timeoutRate >= 0.2) {
      checks.push({
        name: 'Timeout rate trend',
        status: 'fail',
        detail: `Timeout rate ${timeoutRate.toFixed(3)} exceeded 0.200 over the window.`,
      })
    } else if (timeoutRate >= 0.05) {
      checks.push({
        name: 'Timeout rate trend',
        status: 'warn',
        detail: `Timeout rate elevated at ${timeoutRate.toFixed(3)} over the window.`,
      })
    } else {
      checks.push({
        name: 'Timeout rate trend',
        status: 'pass',
        detail: `Timeout rate healthy at ${timeoutRate.toFixed(3)} over the window.`,
      })
    }

    if (failedRunRatio >= 0.2) {
      checks.push({
        name: 'Failed shard trend',
        status: 'fail',
        detail: `Failed run ratio ${failedRunRatio.toFixed(3)} (${failedRuns}/${logs.length}) exceeded 0.200.`,
      })
    } else if (failedRuns > 0) {
      checks.push({
        name: 'Failed shard trend',
        status: 'warn',
        detail: `Observed failed runs in window (${failedRuns}/${logs.length}).`,
      })
    } else {
      checks.push({
        name: 'Failed shard trend',
        status: 'pass',
        detail: 'No failed shard runs in the monitoring window.',
      })
    }
  }

  // 2) Supabase retention checks (older-than-48h rows should be pruned)
  const { count: rideOlderCount, error: rideOlderError } = await supabase
    .from('ride_wait_time_history')
    .select('id', { head: true, count: 'exact' })
    .lt('recorded_at', retentionCutoffIso)

  const { count: weatherOlderCount, error: weatherOlderError } = await supabase
    .from('park_weather_history')
    .select('id', { head: true, count: 'exact' })
    .lt('recorded_at', retentionCutoffIso)

  if (rideOlderError || weatherOlderError) {
    checks.push({
      name: 'Supabase retention window',
      status: 'fail',
      detail: `Unable to verify retention counts: ride=${rideOlderError?.message ?? 'ok'}, weather=${weatherOlderError?.message ?? 'ok'}`,
    })
  } else {
    const olderRide = Number(rideOlderCount ?? 0)
    const olderWeather = Number(weatherOlderCount ?? 0)
    summary.supabaseRetention = {
      cutoffIso: retentionCutoffIso,
      rideOlderThanCutoff: olderRide,
      weatherOlderThanCutoff: olderWeather,
    }

    if (olderRide > 0 || olderWeather > 0) {
      checks.push({
        name: 'Supabase retention window',
        status: 'fail',
        detail: `Rows older than ${retentionHours}h remain (ride=${olderRide}, weather=${olderWeather}).`,
      })
    } else {
      checks.push({
        name: 'Supabase retention window',
        status: 'pass',
        detail: `No Supabase rows older than ${retentionHours}h remain in hot tables.`,
      })
    }
  }

  // 3) Turso long-term checks (optional if creds absent)
  if (!tursoUrl || !tursoToken) {
    checks.push({
      name: 'Turso long-term history',
      status: 'warn',
      detail: 'TURSO_DATABASE_URL/TURSO_AUTH_TOKEN not configured for monitoring; skipping direct Turso checks.',
    })
  } else {
    try {
      const turso = createTursoClient({ url: tursoUrl, authToken: tursoToken })

      const rideAgg = await turso.execute(
        `SELECT COUNT(*) AS row_count, MIN(recorded_at) AS min_recorded_at, MAX(recorded_at) AS max_recorded_at FROM ride_wait_time_history`
      )
      const weatherAgg = await turso.execute(
        `SELECT COUNT(*) AS row_count, MIN(recorded_at) AS min_recorded_at, MAX(recorded_at) AS max_recorded_at FROM park_weather_history`
      )

      const rideRow = (rideAgg.rows?.[0] ?? {}) as Record<string, any>
      const weatherRow = (weatherAgg.rows?.[0] ?? {}) as Record<string, any>

      const rideMin = String(rideRow.min_recorded_at ?? '')
      const rideMax = String(rideRow.max_recorded_at ?? '')
      const weatherMin = String(weatherRow.min_recorded_at ?? '')
      const weatherMax = String(weatherRow.max_recorded_at ?? '')
      const rideCount = Number(rideRow.row_count ?? 0)
      const weatherCount = Number(weatherRow.row_count ?? 0)

      summary.turso = {
        ride: { rowCount: rideCount, minRecordedAt: rideMin || null, maxRecordedAt: rideMax || null },
        weather: { rowCount: weatherCount, minRecordedAt: weatherMin || null, maxRecordedAt: weatherMax || null },
      }

      if (!rideMax || !weatherMax) {
        checks.push({
          name: 'Turso long-term history',
          status: 'fail',
          detail: 'Turso tables missing max(recorded_at) values; historical writes may be broken.',
        })
      } else {
        const rideMaxAgeMinutes = (Date.now() - Date.parse(rideMax)) / (1000 * 60)
        const weatherMaxAgeMinutes = (Date.now() - Date.parse(weatherMax)) / (1000 * 60)
        const hasLongRange = Boolean(rideMin && Date.parse(rideMin) < Date.parse(retentionCutoffIso))

        if (!Number.isFinite(rideMaxAgeMinutes) || !Number.isFinite(weatherMaxAgeMinutes)) {
          checks.push({
            name: 'Turso long-term history',
            status: 'fail',
            detail: 'Turso max(recorded_at) values are not parseable timestamps.',
          })
        } else if (rideMaxAgeMinutes > maxFreshnessMinutes * 2 || weatherMaxAgeMinutes > maxFreshnessMinutes * 2) {
          checks.push({
            name: 'Turso long-term history',
            status: 'fail',
            detail: `Turso is stale (ride max age ${rideMaxAgeMinutes.toFixed(1)}m, weather max age ${weatherMaxAgeMinutes.toFixed(1)}m).`,
          })
        } else if (!hasLongRange) {
          checks.push({
            name: 'Turso long-term history',
            status: 'warn',
            detail: `Turso has recent writes but no rows older than ${retentionHours}h yet (possibly expected during initial rollout).`,
          })
        } else {
          checks.push({
            name: 'Turso long-term history',
            status: 'pass',
            detail: 'Turso max timestamps are fresh and long-range rows are present beyond Supabase hot window.',
          })
        }
      }
    } catch (error: any) {
      checks.push({
        name: 'Turso long-term history',
        status: 'fail',
        detail: `Failed Turso verification: ${String(error?.message ?? error)}`,
      })
    }
  }

  // 4) Per-park freshness percentiles from latest weather points
  const { data: parks, error: parksError } = await supabase
    .from('parks')
    .select('id, queue_times_id')
    .not('queue_times_id', 'is', null)

  if (parksError || !parks) {
    checks.push({
      name: 'Per-park freshness percentiles',
      status: 'fail',
      detail: `Unable to load parks: ${parksError?.message ?? 'unknown error'}`,
    })
  } else {
    const parkIds = new Set(parks.map((park) => String(park.id)))
    const { data: weatherRows, error: weatherError } = await supabase
      .from('park_weather_history')
      .select('park_id, recorded_at')
      .order('recorded_at', { ascending: false })
      .limit(Math.max(5000, parkIds.size * 150))

    if (weatherError || !weatherRows) {
      checks.push({
        name: 'Per-park freshness percentiles',
        status: 'fail',
        detail: `Unable to load weather history: ${weatherError?.message ?? 'unknown error'}`,
      })
    } else {
      const latestByPark = new Map<string, number>()
      for (const row of weatherRows) {
        const parkId = String((row as any).park_id)
        if (!parkIds.has(parkId) || latestByPark.has(parkId)) continue
        const timestamp = Date.parse(String((row as any).recorded_at))
        if (Number.isFinite(timestamp)) latestByPark.set(parkId, timestamp)
        if (latestByPark.size >= parkIds.size) break
      }

      const nowMs = Date.now()
      const agesMinutes: number[] = []
      for (const parkId of parkIds) {
        const latest = latestByPark.get(parkId)
        if (latest == null) {
          agesMinutes.push(Number.POSITIVE_INFINITY)
        } else {
          agesMinutes.push((nowMs - latest) / (1000 * 60))
        }
      }

      const finiteAges = agesMinutes.filter((value) => Number.isFinite(value))
      const p50 = percentile(finiteAges, 50)
      const p90 = percentile(finiteAges, 90)
      const p95 = percentile(finiteAges, 95)
      const missingParks = agesMinutes.length - finiteAges.length

      summary.freshness = {
        parksTracked: parkIds.size,
        parksWithRecentData: finiteAges.length,
        parksMissingRecentData: missingParks,
        p50Minutes: Number.isFinite(p50) ? Number(p50.toFixed(2)) : null,
        p90Minutes: Number.isFinite(p90) ? Number(p90.toFixed(2)) : null,
        p95Minutes: Number.isFinite(p95) ? Number(p95.toFixed(2)) : null,
      }

      if (!Number.isFinite(p95) || p95 > maxFreshnessMinutes) {
        checks.push({
          name: 'Per-park freshness percentiles',
          status: 'fail',
          detail: `P95 freshness ${Number.isFinite(p95) ? p95.toFixed(1) : 'inf'}m exceeded ${maxFreshnessMinutes}m (missing parks=${missingParks}).`,
        })
      } else if (p90 > maxFreshnessMinutes) {
        checks.push({
          name: 'Per-park freshness percentiles',
          status: 'warn',
          detail: `P90 freshness ${p90.toFixed(1)}m exceeded ${maxFreshnessMinutes}m target (missing parks=${missingParks}).`,
        })
      } else {
        checks.push({
          name: 'Per-park freshness percentiles',
          status: 'pass',
          detail: `Freshness healthy (P50=${p50.toFixed(1)}m, P90=${p90.toFixed(1)}m, P95=${p95.toFixed(1)}m).`,
        })
      }
    }
  }

  // 5) API contract checks (park / ride / weather)
  const { data: sampleParkRow } = await supabase
    .from('parks')
    .select('id, queue_times_id')
    .not('queue_times_id', 'is', null)
    .order('name', { ascending: true })
    .limit(1)
    .maybeSingle()

  let sampleRideRow: { queue_times_id: number | null } | null = null
  if (sampleParkRow?.id) {
    const { data: rideRow } = await supabase
      .from('ride_metadata')
      .select('queue_times_id')
      .eq('park_id', sampleParkRow.id)
      .not('queue_times_id', 'is', null)
      .order('name', { ascending: true })
      .limit(1)
      .maybeSingle()
    sampleRideRow = rideRow
  }

  if (!sampleParkRow?.queue_times_id || !sampleRideRow?.queue_times_id) {
    checks.push({
      name: 'History API contract checks',
      status: 'warn',
      detail: 'Could not resolve sample park/ride IDs for contract checks.',
    })
  } else {
    const parkQueueTimesId = Number(sampleParkRow.queue_times_id)
    const rideQueueTimesId = Number(sampleRideRow.queue_times_id)

    const parkHistory = await fetchJson(`${baseUrl}/api/parks/${parkQueueTimesId}/history?days=7&granularity=hourly`)
    const rideHistory = await fetchJson(`${baseUrl}/api/rides/${rideQueueTimesId}/history?days=7&granularity=raw`)
    const weatherHistory = await fetchJson(`${baseUrl}/api/weather/${parkQueueTimesId}/correlation?days=7`)

    const requiredMetaFields = ['raw_records_total', 'raw_records_truncated', 'page_count', 'data_source_breakdown']

    const failures: string[] = []
    const parkMissing = hasMetaFields(parkHistory.json?.meta, requiredMetaFields)
    const rideMissing = hasMetaFields(rideHistory.json?.meta, requiredMetaFields)
    const weatherMissing = hasMetaFields(weatherHistory.json?.meta, requiredMetaFields)

    if (!parkHistory.ok) failures.push(`park history HTTP ${parkHistory.status}`)
    if (!rideHistory.ok) failures.push(`ride history HTTP ${rideHistory.status}`)
    if (!weatherHistory.ok) failures.push(`weather correlation HTTP ${weatherHistory.status}`)
    if (parkMissing.length > 0) failures.push(`park history missing meta: ${parkMissing.join(', ')}`)
    if (rideMissing.length > 0) failures.push(`ride history missing meta: ${rideMissing.join(', ')}`)
    if (weatherMissing.length > 0) failures.push(`weather correlation missing meta: ${weatherMissing.join(', ')}`)

    if (failures.length > 0) {
      checks.push({
        name: 'History API contract checks',
        status: 'fail',
        detail: failures.join(' | '),
      })
    } else {
      checks.push({
        name: 'History API contract checks',
        status: 'pass',
        detail: 'Park, ride, and weather history APIs returned expected metadata contract fields.',
      })
    }
  }

  // 6) Collector contract quick check
  if (!cronSecret) {
    checks.push({
      name: 'Collector quick contract check',
      status: 'warn',
      detail: 'CRON_SECRET not configured; skipped quick collector contract check.',
    })
  } else {
    const nowEpoch = Math.floor(Date.now() / 1000)
    const tickEpoch = nowEpoch - (nowEpoch % (expectedTickMinutes * 60))
    const tickBucket = new Date(tickEpoch * 1000).toISOString()

    const quickUrl = `${baseUrl}/api/cron/collect-queue-times?secret=${encodeURIComponent(
      cronSecret
    )}&quick=true&shard_index=0&shard_total=${expectedShardTotal}&tick_bucket=${encodeURIComponent(tickBucket)}`

    const quickResp = await fetchJson(quickUrl)
    const missing: string[] = []
    for (const field of ['tick_bucket', 'shard_index', 'shard_total', 'coverage_expected_parks', 'coverage_processed_parks']) {
      if (!(field in (quickResp.json ?? {}))) missing.push(field)
    }

    if (!quickResp.ok || quickResp.json?.contractVersion !== 'collect-queue-times/v1' || missing.length > 0) {
      checks.push({
        name: 'Collector quick contract check',
        status: 'fail',
        detail: `HTTP=${quickResp.status}, contract=${quickResp.json?.contractVersion ?? 'missing'}, missing=[${missing.join(', ')}]`,
      })
    } else {
      checks.push({
        name: 'Collector quick contract check',
        status: 'pass',
        detail: 'Collector quick endpoint returned expected additive shard/coverage fields.',
      })
    }
  }

  const failCount = checks.filter((check) => check.status === 'fail').length
  const warnCount = checks.filter((check) => check.status === 'warn').length

  const reportLines = [
    '# Collector Monitoring Report',
    '',
    `Generated: ${new Date().toISOString()}`,
    `Base URL: ${baseUrl}`,
    `Lookback: ${lookbackMinutes} minutes`,
    '',
    '## Checks',
    ...checks.map((check) => `- [${formatStatus(check.status)}] ${check.name}: ${check.detail}`),
    '',
    '## Summary',
    `- Fails: ${failCount}`,
    `- Warnings: ${warnCount}`,
    '',
    '## Metrics',
    '```json',
    JSON.stringify(summary, null, 2),
    '```',
    '',
  ]

  fs.writeFileSync(reportFile, reportLines.join('\n'), 'utf8')
  console.log(reportLines.join('\n'))

  if (failCount > 0) {
    process.exit(1)
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
