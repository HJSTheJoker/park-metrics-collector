#!/usr/bin/env node

/**
 * Enhanced park metrics collection with dual API support
 * NOW WRITES TO TURSODB for historical data storage
 */

import * as dotenv from 'dotenv'
import { supabase, writeWaitTimesToTurso, writeWeatherToTurso } from './lib/database-clients'
import { themeParksWiki } from './lib/themeparks-wiki'
import { aggregator } from './lib/aggregator'
import * as fs from 'fs'

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

// Load park mappings
const mappings = JSON.parse(fs.readFileSync('./mappings.json', 'utf-8'))
const parkMappings = new Map(
  mappings.parks.map((p: any) => [p.queue_times_id, p.themeparks_id])
)

// Weather data collection (unchanged)
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

// Queue-Times data collection
async function collectQueueTimes(parkId: number) {
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
    console.error(`Queue-Times error for park ${parkId}:`, error)
    return null
  }
}

// ThemeParks.wiki data collection
async function collectThemeParks(parkId: string) {
  try {
    const attractions = await themeParksWiki.getParkWaitTimes(parkId)

    if (!attractions) return null

    // Convert to simple format
    const converted = attractions
      .map(a => themeParksWiki.convertToSimpleFormat(a))
      .filter(Boolean)

    return converted
  } catch (error) {
    console.error(`ThemeParks.wiki error for park ${parkId}:`, error)
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

async function runEnhancedCollection() {
  const startTime = Date.now()
  console.log('Starting enhanced data collection with dual API support...')
  console.log(`Time: ${new Date().toISOString()}`)
  console.log('🗃️  Writing to TursoDB for historical data storage')

  const stats = {
    locations: 0,
    processed: 0,
    weather: 0,
    queueTimesData: 0,
    themeparksData: 0,
    aggregated: 0,
    stored_wait_times: 0,
    stored_weather: 0,
    highConfidence: 0,
    mediumConfidence: 0,
    lowConfidence: 0,
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

    // Get metadata mapping for rides from Supabase
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

    // Get ride mappings from Supabase
    const { data: rideMappings } = await supabase
      .from('ride_mappings')
      .select('queue_times_id, themeparks_id')

    const rideMap = new Map(
      rideMappings?.map(r => [r.queue_times_id, r.themeparks_id]) || []
    )

    const timestamp = new Date().toISOString()
    const weatherRecords = []
    const waitTimeRecords = []

    // Process all locations
    for (const location of locations) {
      process.stdout.write(`Processing ${location.name}...`)

      try {
        let queueTimesData: any[] = []
        let themeparksData: any[] = []

        // Collect weather if coordinates available
        if (location.lat && location.lon) {
          const weatherData = await collectWeather(location.lat, location.lon)
          if (weatherData) {
            weatherRecords.push({
              location_id: location.id,
              park_id: location.id, // Add park_id for TursoDB
              temperature: weatherData.temp,
              feels_like: weatherData.feels,
              precipitation: weatherData.precip,
              humidity: weatherData.humid,
              wind_speed: weatherData.wind_s,
              uv_index: weatherData.uv,
              weather_code: weatherData.code,
              weather_description: getWeatherType(weatherData.code || 0),
              recorded_at: timestamp,
              source: 'open_meteo'
            })
            stats.weather++
          }
        }

        // Collect from both queue APIs if external ID available
        if (location.external_id) {
          // Queue-Times collection
          const queueData = await collectQueueTimes(location.external_id)
          if (queueData && queueData.length > 0) {
            queueTimesData = queueData
            stats.queueTimesData += queueData.length
          }

          // ThemeParks.wiki collection if mapping exists
          const themeparksId = parkMappings.get(location.external_id)
          if (themeparksId) {
            const tpData = await collectThemeParks(themeparksId)
            if (tpData && tpData.length > 0) {
              themeparksData = tpData
              stats.themeparksData += tpData.length
            }
          }
        }

        // Aggregate wait times if we have ride data
        if (queueTimesData.length > 0 || themeparksData.length > 0) {
          const aggregatedData = aggregator.processRideData(
            queueTimesData,
            themeparksData,
            rideMap
          )

          stats.aggregated += aggregatedData.length

          // Prepare wait time records for TursoDB
          for (const ride of aggregatedData) {
            const metaId = metaMap.get(parseInt(ride.rideId))
            if (!metaId) continue

            // Count confidence levels
            if (ride.confidenceScore >= 0.8) stats.highConfidence++
            else if (ride.confidenceScore >= 0.6) stats.mediumConfidence++
            else stats.lowConfidence++

            waitTimeRecords.push({
              id: crypto.randomUUID(),
              item_id: metaId,
              park_id: location.id,
              wait_time: ride.aggregatedWait,
              queue_times_wait: ride.queueTimesWait,
              themeparks_wait: ride.themeparksWait,
              confidence: ride.confidenceScore,
              is_open: ride.isOpen !== false,
              source: (queueTimesData.length > 0 && themeparksData.length > 0) ? 'dual' :
                      queueTimesData.length > 0 ? 'queue_times' : 'themeparks',
              recorded_at: timestamp
            })
          }
        }

        const elapsed = Date.now() - startTime
        console.log(` [WQ] ${elapsed}ms`)
        stats.processed++

      } catch (error) {
        console.log(' [ERROR]')
        console.error(error)
        stats.errors++
      }
    }

    // Write data to TursoDB in batches
    console.log('\n📝 Writing data to TursoDB...')

    if (weatherRecords.length > 0) {
      console.log(`Writing ${weatherRecords.length} weather records...`)
      const weatherResult = await writeWeatherToTurso(weatherRecords)
      stats.stored_weather = weatherResult.inserted
      console.log(`✅ Weather: ${weatherResult.inserted}/${weatherResult.total} records written`)
    }

    if (waitTimeRecords.length > 0) {
      console.log(`Writing ${waitTimeRecords.length} wait time records...`)
      const waitResult = await writeWaitTimesToTurso(waitTimeRecords)
      stats.stored_wait_times = waitResult.inserted
      console.log(`✅ Wait times: ${waitResult.inserted}/${waitResult.total} records written`)
    }

    // Summary
    const totalTime = Date.now() - startTime
    console.log('---')
    console.log(`Complete in ${(totalTime / 1000).toFixed(1)}s`)
    console.log(`Processed: ${stats.processed}/${stats.locations}`)
    console.log(`Weather collected: ${stats.weather}`)
    console.log(`Weather stored: ${stats.stored_weather}`)
    console.log(`Queue-Times rides: ${stats.queueTimesData}`)
    console.log(`ThemeParks.wiki rides: ${stats.themeparksData}`)
    console.log(`Aggregated rides: ${stats.aggregated}`)
    console.log(`Wait times stored: ${stats.stored_wait_times}`)
    console.log(`Confidence - High: ${stats.highConfidence}, Medium: ${stats.mediumConfidence}, Low: ${stats.lowConfidence}`)
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
runEnhancedCollection()
