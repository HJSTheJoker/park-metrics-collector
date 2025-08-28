#!/usr/bin/env node

/**
 * Generic park metrics collection utility
 * Aggregates publicly available queue time data
 */

import * as dotenv from 'dotenv'
import { createClient } from '@supabase/supabase-js'

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
  
  const stats = {
    locations: 0,
    processed: 0,
    weather: 0,
    queues: 0,
    stored: 0,
    errors: 0
  }

  try {
    // Get locations from database
    const { data: locations, error: locError } = await db
      .from('locations')
      .select('id, name, external_id, lat, lon')
      .order('name')

    if (locError || !locations) {
      console.error('Database error')
      process.exit(1)
    }

    stats.locations = locations.length
    console.log(`Found ${locations.length} locations`)

    // Get metadata mapping
    const { data: metadata, error: metaError } = await db
      .from('metadata')
      .select('id, external_id')

    if (metaError) {
      console.error('Metadata error')
      process.exit(1)
    }

    const metaMap = new Map(
      metadata?.map(m => [m.external_id, m.id]) || []
    )

    const timestamp = new Date().toISOString()

    // Process all locations
    for (const location of locations) {
      process.stdout.write(`Processing ${location.name}...`)

      try {
        const tasks = []

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
                    wind_direction: data.wind_d,
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

        // Collect queue data if external ID available
        if (location.external_id) {
          tasks.push(
            collectQueues(location.external_id).then(async (items) => {
              if (items && items.length > 0) {
                stats.queues += items.length

                const records = items
                  .map(item => {
                    const metaId = metaMap.get(item.id)
                    if (!metaId) return null

                    return {
                      item_id: metaId,
                      wait_time: item.wait_time || 0,
                      is_open: item.is_open !== false,
                      source: 'queue_times',
                      confidence: 1.0,
                      recorded_at: timestamp
                    }
                  })
                  .filter(Boolean)

                if (records.length > 0) {
                  const { error } = await db
                    .from('wait_times')
                    .insert(records)
                  
                  if (!error) {
                    stats.stored += records.length
                    return 'Q'
                  }
                }
              }
              return null
            })
          )
        }

        const results = await Promise.all(tasks)
        const success = results.filter(Boolean).join('')
        
        const elapsed = Date.now() - startTime
        console.log(` [${success || '-'}] ${elapsed}ms`)
        
        stats.processed++
      } catch (error) {
        console.log(' [ERROR]')
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
    console.log(`Queues: ${stats.queues}`)
    console.log(`Stored: ${stats.stored}`)
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