#!/usr/bin/env node

/**
 * Generic park metrics collection utility
 * Aggregates publicly available queue time data
 * DUAL-WRITE: Writes to BOTH TursoDB and Supabase for transition period
 */

import * as dotenv from 'dotenv'
import {
  supabase,
  writeWaitTimesToTurso,
  writeWeatherToTurso,
  writeWaitTimesToSupabase,
  writeWeatherToSupabase,
  isDualWriteEnabled
} from './lib/database-clients'

// Load environment
dotenv.config()

// Generic environment variables (for backward compatibility)
const DATABASE_URL = process.env.DB_CONNECTION
const DATABASE_KEY = process.env.DB_AUTH

// New TursoDB environment variables
const TURSO_URL = process.env.TURSO_DATABASE_URL || process.env.TURSO_DB_URL
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN || process.env.TURSO_AUTH_TOKEN

if (!DATABASE_URL || !DATABASE_KEY || !TURSO_URL || !TURSO_TOKEN) {
  console.error('Missing required configuration')
  console.error('Need: DB_CONNECTION, DB_AUTH, TURSO_DATABASE_URL, TURSO_AUTH_TOKEN')
  process.exit(1)
}

// Weather data collection
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
      uv: data.current?.uv_index
    }
  } catch (error) {
    return null
  }
}

// Queue data collection
async function collectQueues(parkId: number) {
  try {
    const response = await fetch(`https://queue-times.com/parks/${parkId}/queue_times.json`)

    if (!response.ok) return null

    const data = await response.json()

    const allItems = []

    if (data.rides && Array.isArray(data.rides)) {
      allItems.push(...data.rides)
    }

    if (data.lands && Array.isArray(data.lands)) {
      data.lands.forEach((land: any) => {
        if (land.rides && Array.isArray(land.rides)) {
          allItems.push(...land.rides)
        }
      })
    }

    const unique = Array.from(
      new Map(allItems.map(item => [item.id, item])).values()
    )

    return unique
  } catch (error) {
    return null
  }
}

// Weather codes mapping
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
    99: 'severe_thunderstorm'
  }
  return types[code] || 'unknown'
}

async function runCollection() {
  const startTime = Date.now()
  console.log('Starting data collection...')
  console.log(`Time: ${new Date().toISOString()}`)
  console.log('🗃️  Writing to TursoDB for historical data storage')

  const dualWriteActive = isDualWriteEnabled()
  if (dualWriteActive) {
    console.log('📦 DUAL-WRITE ENABLED: Also writing to Supabase')
  } else {
    console.log('⚠️  Supabase dual-write disabled (add SUPABASE_SERVICE_ROLE_KEY to enable)')
  }

  const stats = {
    locations: 0,
    processed: 0,
    weather: 0,
    queues: 0,
    // Turso stats
    turso_wait_times: 0,
    turso_weather: 0,
    // Supabase stats
    supabase_wait_times: 0,
    supabase_weather: 0,
    errors: 0
  }

  try {
    // Get locations from Supabase (reference data)
    const { data: locations, error: locError } = await supabase
      .from('locations')
      .select('id, name, external_id, lat, lon')
      .order('name')

    if (locError || !locations) {
      console.error('Supabase error fetching locations')
      process.exit(1)
    }

    stats.locations = locations.length
    console.log(`Found ${locations.length} locations from Supabase`)

    // Get metadata mapping from Supabase
    const { data: metadata, error: metaError } = await supabase
      .from('metadata')
      .select('id, external_id')

    if (metaError) {
      console.error('Supabase error fetching metadata')
      process.exit(1)
    }

    const metaMap = new Map(
      metadata?.map(m => [m.external_id, m.id]) || []
    )

    const timestamp = new Date().toISOString()
    const weatherRecords = []
    const waitTimeRecords = []

    // Process all locations
    for (const location of locations) {
      process.stdout.write(`Processing ${location.name}...`)

      try {
        // Collect weather if coordinates available
        if (location.lat && location.lon) {
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
              source: 'open_meteo'
            })
            stats.weather++
          }
        }

        // Collect queue data if external ID available
        if (location.external_id) {
          const queueItems = await collectQueues(location.external_id)
          if (queueItems && queueItems.length > 0) {
            stats.queues += queueItems.length

            const records = queueItems
              .map(item => {
                const metaId = metaMap.get(item.id)
                if (!metaId) return null

                return {
                  id: crypto.randomUUID(),
                  item_id: metaId,
                  park_id: location.id,
                  wait_time: item.wait_time || 0,
                  is_open: item.is_open !== false,
                  source: 'queue_times',
                  confidence: 1.0,
                  recorded_at: timestamp
                }
              })
              .filter(Boolean)

            waitTimeRecords.push(...records)
          }
        }

        const elapsed = Date.now() - startTime
        console.log(` [WQ] ${elapsed}ms`)
        stats.processed++

      } catch (error) {
        console.log(' [ERROR]')
        stats.errors++
      }
    }

    // Write data to TursoDB first
    console.log('\n📝 Writing data to TursoDB...')

    if (weatherRecords.length > 0) {
      console.log(`Writing ${weatherRecords.length} weather records to Turso...`)
      const weatherResult = await writeWeatherToTurso(weatherRecords)
      stats.turso_weather = weatherResult.inserted
      console.log(`✅ Turso Weather: ${weatherResult.inserted}/${weatherResult.total} records written`)
    }

    if (waitTimeRecords.length > 0) {
      console.log(`Writing ${waitTimeRecords.length} wait time records to Turso...`)
      const waitResult = await writeWaitTimesToTurso(waitTimeRecords)
      stats.turso_wait_times = waitResult.inserted
      console.log(`✅ Turso Wait times: ${waitResult.inserted}/${waitResult.total} records written`)
    }

    // DUAL-WRITE: Also write to Supabase if enabled
    if (dualWriteActive) {
      console.log('\n📝 Writing data to Supabase (dual-write)...')

      if (weatherRecords.length > 0) {
        console.log(`Writing ${weatherRecords.length} weather records to Supabase...`)
        const supabaseWeatherResult = await writeWeatherToSupabase(weatherRecords)
        stats.supabase_weather = supabaseWeatherResult.inserted
        console.log(`✅ Supabase Weather: ${supabaseWeatherResult.inserted}/${supabaseWeatherResult.total} records written`)
      }

      if (waitTimeRecords.length > 0) {
        console.log(`Writing ${waitTimeRecords.length} wait time records to Supabase...`)
        const supabaseWaitResult = await writeWaitTimesToSupabase(waitTimeRecords)
        stats.supabase_wait_times = supabaseWaitResult.inserted
        console.log(`✅ Supabase Wait times: ${supabaseWaitResult.inserted}/${supabaseWaitResult.total} records written`)
      }
    }

    // Summary
    const totalTime = Date.now() - startTime
    console.log('\n---')
    console.log(`Complete in ${(totalTime / 1000).toFixed(1)}s`)
    console.log(`Processed: ${stats.processed}/${stats.locations}`)
    console.log(`Weather collected: ${stats.weather}`)
    console.log(`Queues collected: ${stats.queues}`)
    console.log('\n📊 Database Writes:')
    console.log(`  TursoDB:`)
    console.log(`    - Weather: ${stats.turso_weather}`)
    console.log(`    - Wait times: ${stats.turso_wait_times}`)
    if (dualWriteActive) {
      console.log(`  Supabase:`)
      console.log(`    - Weather: ${stats.supabase_weather}`)
      console.log(`    - Wait times: ${stats.supabase_wait_times}`)
    } else {
      console.log(`  Supabase: DISABLED`)
    }
    if (stats.errors > 0) {
      console.log(`Errors: ${stats.errors}`)
    }

    process.exit(0)

  } catch (error) {
    console.error('Fatal error:', error)
    process.exit(1)
  }
}

// Run
runCollection()
