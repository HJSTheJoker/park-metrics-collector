#!/usr/bin/env node

/**
 * Enhanced park metrics collection with dual API support
 * Combines Queue-Times and ThemeParks.wiki data for improved accuracy
 */

import * as dotenv from 'dotenv'
import { createClient } from '@supabase/supabase-js'
import { themeParksWiki } from './lib/themeparks-wiki'
import { aggregator } from './lib/aggregator'
import * as fs from 'fs'

// Load environment
dotenv.config()

// Generic environment variables
const DATABASE_URL = process.env.DB_CONNECTION
const DATABASE_KEY = process.env.DB_AUTH

if (!DATABASE_URL || !DATABASE_KEY) {
  console.error('Missing required configuration')
  process.exit(1)
}

const db = createClient(DATABASE_URL, DATABASE_KEY)

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
  
  const stats = {
    locations: 0,
    processed: 0,
    weather: 0,
    queueTimesData: 0,
    themeparksData: 0,
    aggregated: 0,
    stored: 0,
    highConfidence: 0,
    mediumConfidence: 0,
    lowConfidence: 0,
    errors: 0
  }

  try {
    // Get locations from database
    const { data: locations, error: locError } = await db
      .from('locations')
      .select('id, name, external_id, lat, lon')
      .order('name')

    if (locError || !locations) {
      console.error('Database error:', locError)
      process.exit(1)
    }

    stats.locations = locations.length
    console.log(`Found ${locations.length} locations`)

    // Get metadata mapping for rides
    const { data: metadata, error: metaError } = await db
      .from('metadata')
      .select('id, external_id')

    if (metaError) {
      console.error('Metadata error:', metaError)
      process.exit(1)
    }

    const metaMap = new Map(
      metadata?.map(m => [m.external_id, m.id]) || []
    )

    // Get ride mappings (if exists)
    const { data: rideMappings } = await db
      .from('ride_mappings')
      .select('queue_times_id, themeparks_id')
    
    const rideMap = new Map(
      rideMappings?.map(r => [r.queue_times_id, r.themeparks_id]) || []
    )

    const timestamp = new Date().toISOString()

    // Process all locations
    for (const location of locations) {
      process.stdout.write(`Processing ${location.name}...`)

      try {
        const tasks = []
        let queueTimesData: any[] = []
        let themeparksData: any[] = []

        // Collect weather if coordinates available
        if (location.lat && location.lon) {
          tasks.push(
            collectWeather(location.lat, location.lon).then(async (data) => {
              if (data) {
                const { error } = await db
                  .from('weather_data')
                  .insert({
                    location_id: location.id,
                    temperature: data.temp,
                    feels_like: data.feels,
                    precipitation: data.precip,
                    humidity: data.humid,
                    wind_speed: data.wind_s,
                    uv_index: data.uv,
                    weather_code: data.code,
                    weather_type: getWeatherType(data.code || 0),
                    recorded_at: timestamp,
                    source: 'open_meteo'
                  })
                
                if (!error) {
                  stats.weather++
                  return 'W'
                }
              }
              return null
            })
          )
        }

        // Collect from both queue APIs if external ID available
        if (location.external_id) {
          // Queue-Times collection
          tasks.push(
            collectQueueTimes(location.external_id).then(data => {
              if (data && data.length > 0) {
                queueTimesData = data
                stats.queueTimesData += data.length
                return 'Q'
              }
              return null
            })
          )

          // ThemeParks.wiki collection if mapping exists
          const themeparksId = parkMappings.get(location.external_id)
          if (themeparksId) {
            tasks.push(
              collectThemeParks(themeparksId).then(data => {
                if (data && data.length > 0) {
                  themeparksData = data
                  stats.themeparksData += data.length
                  return 'T'
                }
                return null
              })
            )
          }
        }

        const results = await Promise.all(tasks)
        const sources = results.filter(Boolean).join('')

        // Aggregate wait times if we have ride data
        if (queueTimesData.length > 0 || themeparksData.length > 0) {
          const aggregatedData = aggregator.processRideData(
            queueTimesData,
            themeparksData,
            rideMap
          )

          stats.aggregated += aggregatedData.length

          // Store aggregated data
          for (const ride of aggregatedData) {
            const metaId = metaMap.get(parseInt(ride.rideId))
            if (!metaId) continue

            // Count confidence levels
            if (ride.confidenceScore >= 0.8) stats.highConfidence++
            else if (ride.confidenceScore >= 0.6) stats.mediumConfidence++
            else stats.lowConfidence++

            const { error } = await db
              .from('wait_times')
              .insert({
                item_id: metaId,
                wait_time: ride.aggregatedWait,
                queue_times_wait: ride.queueTimesWait,
                themeparks_wait: ride.themeparksWait,
                confidence_score: ride.confidenceScore,
                is_open: ride.isOpen,
                single_rider_time: ride.singleRiderTime,
                source: sources.includes('Q') && sources.includes('T') ? 'dual' : 
                        sources.includes('Q') ? 'queue_times' : 'themeparks',
                recorded_at: timestamp
              })
            
            if (!error) {
              stats.stored++
            }
          }
        }
        
        const elapsed = Date.now() - startTime
        console.log(` [${sources || '-'}] ${elapsed}ms`)
        
        stats.processed++
      } catch (error) {
        console.log(' [ERROR]')
        console.error(error)
        stats.errors++
      }
    }

    // Cleanup old data
    const { error: cleanupErr } = await db.rpc('cleanup_old_data')
    if (!cleanupErr) {
      console.log('Cleaned old records')
    }

    // Aggregate stats
    const { error: aggregateErr } = await db.rpc('aggregate_hourly_stats')
    if (!aggregateErr) {
      console.log('Aggregated hourly statistics')
    }

    // Summary
    const totalTime = Date.now() - startTime
    console.log('---')
    console.log(`Complete in ${(totalTime / 1000).toFixed(1)}s`)
    console.log(`Processed: ${stats.processed}/${stats.locations}`)
    console.log(`Weather: ${stats.weather}`)
    console.log(`Queue-Times rides: ${stats.queueTimesData}`)
    console.log(`ThemeParks.wiki rides: ${stats.themeparksData}`)
    console.log(`Aggregated rides: ${stats.aggregated}`)
    console.log(`Stored: ${stats.stored}`)
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