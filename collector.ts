#!/usr/bin/env node

/**
 * Generic park metrics collection utility
 * Aggregates publicly available queue time data
 * DUAL-WRITE: Writes to BOTH TursoDB and Supabase
 *
 * Execution model:
 * - Runner-native execution (no Vercel function dependency)
 * - Deterministic sharding via SHARD_INDEX/SHARD_TOTAL
 * - Strict dual-write can be enforced via COLLECTOR_STRICT_DUAL_WRITE=true
 */

import * as fs from 'node:fs'
import * as dotenv from 'dotenv'
import {
  supabase,
  writeWaitTimesToTurso,
  writeWeatherToTurso,
  writeWaitTimesToSupabase,
  writeWeatherToSupabase,
  isDualWriteEnabled,
} from './lib/database-clients'

dotenv.config()

const DATABASE_URL = process.env.DB_CONNECTION
const DATABASE_KEY = process.env.DB_AUTH
const TURSO_URL = process.env.TURSO_DATABASE_URL || process.env.TURSO_DB_URL
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN || process.env.TURSO_TOKEN

type Location = {
  id: string
  name: string
  external_id: number | null
  lat: number | null
  lon: number | null
}

type CollectorSummary = {
  contractVersion: 'collector-direct/v1'
  success: boolean
  criticalFailure: boolean
  executionMode: 'direct'
  strictDualWrite: boolean
  tick_bucket: string
  shard_index: number
  shard_total: number
  coverage_expected_parks: number
  coverage_processed_parks: number
  message: string
  stats: Record<string, any>
  timestamp: string
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const raw = process.env[name]
  const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, parsed))
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = (process.env[name] || '').trim().toLowerCase()
  if (!raw) return fallback
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true
  if (['0', 'false', 'no', 'off'].includes(raw)) return false
  return fallback
}

function normalizeTickBucket(raw?: string): string {
  const parsed = raw ? Date.parse(raw) : Date.now()
  const base = Number.isFinite(parsed) ? parsed : Date.now()
  const bucketMs = 5 * 60 * 1000
  return new Date(Math.floor(base / bucketMs) * bucketMs).toISOString()
}

function hashToShard(value: string, shardTotal: number): number {
  let hash = 2166136261
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0) % shardTotal
}

function getWeatherType(code: number): string {
  const types: Record<number, string> = {
    0: 'clear',
    1: 'mostly_clear',
    2: 'partly_cloudy',
    3: 'overcast',
    45: 'foggy',
    48: 'rime_fog',
    51: 'light_drizzle',
    53: 'drizzle',
    55: 'heavy_drizzle',
    61: 'light_rain',
    63: 'rain',
    65: 'heavy_rain',
    71: 'light_snow',
    73: 'snow',
    75: 'heavy_snow',
    77: 'snow_grains',
    80: 'light_showers',
    81: 'showers',
    82: 'heavy_showers',
    85: 'light_snow_showers',
    86: 'snow_showers',
    95: 'thunderstorm',
    96: 'thunderstorm_hail',
    99: 'severe_thunderstorm',
  }
  return types[code] || 'unknown'
}

async function collectWeather(lat: number, lon: number) {
  try {
    const response = await fetch(
      `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,uv_index&timezone=auto&forecast_days=1`
    )

    if (!response.ok) return null

    const data = await response.json()
    return {
      temp: data.current?.temperature_2m,
      feels: data.current?.apparent_temperature,
      humid: data.current?.relative_humidity_2m,
      precip: data.current?.precipitation,
      code: data.current?.weather_code,
      wind_s: data.current?.wind_speed_10m,
      wind_d: data.current?.wind_direction_10m,
      uv: data.current?.uv_index,
    }
  } catch {
    return null
  }
}

async function collectQueues(parkId: number) {
  try {
    const response = await fetch(`https://queue-times.com/parks/${parkId}/queue_times.json`)

    if (!response.ok) return null

    const data = await response.json()

    const allItems: any[] = []
    if (Array.isArray(data.rides)) allItems.push(...data.rides)

    if (Array.isArray(data.lands)) {
      data.lands.forEach((land: any) => {
        if (Array.isArray(land.rides)) allItems.push(...land.rides)
      })
    }

    return Array.from(new Map(allItems.map((item) => [item.id, item])).values())
  } catch {
    return null
  }
}

function writeSummary(summary: CollectorSummary) {
  const payload = JSON.stringify(summary)
  console.log(payload)

  const summaryFile = process.env.COLLECTOR_SUMMARY_FILE
  if (summaryFile) {
    fs.writeFileSync(summaryFile, `${JSON.stringify(summary, null, 2)}\n`, 'utf8')
  }
}

async function runCollection() {
  if (!DATABASE_URL || !DATABASE_KEY || !TURSO_URL || !TURSO_TOKEN) {
    throw new Error('Missing required configuration (DB_CONNECTION, DB_AUTH, TURSO_DATABASE_URL/TURSO_DB_URL, TURSO_AUTH_TOKEN/TURSO_TOKEN).')
  }

  const startTime = Date.now()
  const shardIndex = envInt('SHARD_INDEX', 0, 0, 1024)
  const shardTotal = envInt('SHARD_TOTAL', 1, 1, 1024)
  const strictDualWrite = envBool('COLLECTOR_STRICT_DUAL_WRITE', true)
  const tickBucket = normalizeTickBucket(process.env.TICK_BUCKET)

  const hardErrors: string[] = []
  const softErrors: string[] = []

  const stats: Record<string, any> = {
    locationsTotal: 0,
    locationsTargeted: 0,
    processed: 0,
    queueEligibleLocations: 0,
    weatherEligibleLocations: 0,
    weatherCollected: 0,
    totalRidesCollected: 0,
    tursoWaitInserted: 0,
    tursoWeatherInserted: 0,
    supabaseWaitInserted: 0,
    supabaseWeatherInserted: 0,
    executionTimeMs: 0,
    errors: [],
  }

  const dualWriteActive = isDualWriteEnabled()
  if (strictDualWrite && !dualWriteActive) {
    hardErrors.push('Strict dual-write requires SUPABASE_SERVICE_ROLE_KEY, but dual-write is disabled.')
  }

  // Read reference locations from Supabase.
  const { data: locations, error: locError } = await supabase
    .from('locations')
    .select('id, name, external_id, lat, lon')
    .order('name')

  if (locError || !locations) {
    throw new Error(`Supabase error fetching locations: ${locError?.message ?? 'unknown error'}`)
  }

  const typedLocations = locations as unknown as Location[]
  stats.locationsTotal = typedLocations.length

  const targetedLocations = typedLocations.filter((location) => {
    const shardKey = String(location.external_id ?? location.id)
    return hashToShard(shardKey, shardTotal) === shardIndex
  })

  stats.locationsTargeted = targetedLocations.length
  if (targetedLocations.length === 0) {
    softErrors.push(`No locations mapped to shard ${shardIndex}/${shardTotal}.`)
  }

  const { data: metadata, error: metaError } = await supabase
    .from('metadata')
    .select('id, external_id')

  if (metaError) {
    throw new Error(`Supabase error fetching metadata: ${metaError.message}`)
  }

  const metaMap = new Map((metadata ?? []).map((m: any) => [String(m.external_id), String(m.id)]))

  const timestamp = new Date().toISOString()
  const weatherRecords: any[] = []
  const waitTimeRecords: any[] = []

  for (const location of targetedLocations) {
    process.stdout.write(`Processing ${location.name}...`)

    try {
      if (location.lat != null && location.lon != null) {
        stats.weatherEligibleLocations += 1
        const weatherData = await collectWeather(location.lat, location.lon)
        if (weatherData) {
          weatherRecords.push({
            location_id: location.id,
            temperature: weatherData.temp,
            feels_like: weatherData.feels,
            precipitation: weatherData.precip,
            humidity: weatherData.humid,
            wind_speed: weatherData.wind_s,
            uv_index: weatherData.uv,
            weather_code: weatherData.code,
            weather_type: getWeatherType(weatherData.code || 0),
            recorded_at: timestamp,
            source: 'open_meteo',
          })
          stats.weatherCollected += 1
        }
      }

      if (location.external_id != null) {
        stats.queueEligibleLocations += 1
        const queueItems = await collectQueues(location.external_id)
        if (queueItems && queueItems.length > 0) {
          stats.totalRidesCollected += queueItems.length
          const records = queueItems
            .map((item) => {
              const metaId = metaMap.get(String(item.id))
              if (!metaId) return null
              return {
                id: crypto.randomUUID(),
                item_id: metaId,
                park_id: location.id,
                wait_time: item.wait_time || 0,
                is_open: item.is_open !== false,
                source: 'queue_times',
                recorded_at: timestamp,
              }
            })
            .filter(Boolean)

          waitTimeRecords.push(...records)
        }
      }

      stats.processed += 1
      const elapsed = Date.now() - startTime
      console.log(` [OK] ${elapsed}ms`)
    } catch (error: any) {
      const err = `Location ${location.name} failed: ${String(error?.message ?? error)}`
      stats.errors.push(err)
      console.log(' [ERROR]')
    }
  }

  if (weatherRecords.length > 0) {
    const weatherResult = await writeWeatherToTurso(weatherRecords)
    stats.tursoWeatherInserted = weatherResult.inserted
    if (weatherResult.inserted !== weatherResult.total) {
      hardErrors.push(`Turso weather partial write (${weatherResult.inserted}/${weatherResult.total}).`)
    }
  }

  if (waitTimeRecords.length > 0) {
    const waitResult = await writeWaitTimesToTurso(waitTimeRecords)
    stats.tursoWaitInserted = waitResult.inserted
    if (waitResult.inserted !== waitResult.total) {
      hardErrors.push(`Turso wait-time partial write (${waitResult.inserted}/${waitResult.total}).`)
    }
  }

  if (dualWriteActive) {
    if (weatherRecords.length > 0) {
      const supabaseWeather = await writeWeatherToSupabase(weatherRecords)
      stats.supabaseWeatherInserted = supabaseWeather.inserted
      if (strictDualWrite && supabaseWeather.inserted !== supabaseWeather.total) {
        hardErrors.push(`Supabase weather partial write (${supabaseWeather.inserted}/${supabaseWeather.total}).`)
      }
    }

    if (waitTimeRecords.length > 0) {
      const supabaseWait = await writeWaitTimesToSupabase(waitTimeRecords)
      stats.supabaseWaitInserted = supabaseWait.inserted
      if (strictDualWrite && supabaseWait.inserted !== supabaseWait.total) {
        hardErrors.push(`Supabase wait-time partial write (${supabaseWait.inserted}/${supabaseWait.total}).`)
      }
    }
  } else if (strictDualWrite) {
    hardErrors.push('Supabase dual-write disabled while strict mode is enabled.')
  }

  if (stats.processed === 0 && targetedLocations.length > 0) {
    hardErrors.push('No targeted locations were processed successfully.')
  }

  stats.executionTimeMs = Date.now() - startTime
  stats.errors = [...stats.errors, ...softErrors, ...hardErrors]

  const success = hardErrors.length === 0
  const summary: CollectorSummary = {
    contractVersion: 'collector-direct/v1',
    success,
    criticalFailure: !success,
    executionMode: 'direct',
    strictDualWrite,
    tick_bucket: tickBucket,
    shard_index: shardIndex,
    shard_total: shardTotal,
    coverage_expected_parks: targetedLocations.length,
    coverage_processed_parks: stats.processed,
    message: success
      ? `Processed ${stats.processed} of ${targetedLocations.length} targeted parks in ${stats.executionTimeMs}ms`
      : `Collector failed strict checks for shard ${shardIndex}/${shardTotal}`,
    stats,
    timestamp: new Date().toISOString(),
  }

  writeSummary(summary)

  if (!success) {
    process.exit(1)
  }
}

runCollection().catch((error: any) => {
  const shardIndex = envInt('SHARD_INDEX', 0, 0, 1024)
  const shardTotal = envInt('SHARD_TOTAL', 1, 1, 1024)
  const summary: CollectorSummary = {
    contractVersion: 'collector-direct/v1',
    success: false,
    criticalFailure: true,
    executionMode: 'direct',
    strictDualWrite: envBool('COLLECTOR_STRICT_DUAL_WRITE', true),
    tick_bucket: normalizeTickBucket(process.env.TICK_BUCKET),
    shard_index: shardIndex,
    shard_total: shardTotal,
    coverage_expected_parks: 0,
    coverage_processed_parks: 0,
    message: `Fatal collector error: ${String(error?.message ?? error)}`,
    stats: {
      errors: [String(error?.message ?? error)],
    },
    timestamp: new Date().toISOString(),
  }

  writeSummary(summary)
  process.exit(1)
})
