/**
 * Database clients for dual-database architecture
 * - Supabase: Reference data (parks, rides, locations, metadata)
 * - TursoDB: Historical data (wait times, weather)
 */

import { createClient as createSupabase } from '@supabase/supabase-js'
import { createClient as createTurso, Client } from '@libsql/client'

// Supabase - for reading reference data (parks, rides metadata)
export const supabase = createSupabase(
  process.env.SUPABASE_URL || process.env.DB_CONNECTION!,
  process.env.SUPABASE_KEY || process.env.DB_AUTH!
)

// TursoDB - for writing historical data (wait times, weather)
let tursoClient: Client | null = null

export function getTursoClient(): Client {
  if (tursoClient) return tursoClient

  const url = process.env.TURSO_DATABASE_URL || process.env.TURSO_DB_URL!
  const token = process.env.TURSO_AUTH_TOKEN || process.env.TURSO_AUTH_TOKEN!

  if (!url || !token) {
    throw new Error('TursoDB credentials not found in environment variables')
  }

  tursoClient = createTurso({
    url,
    authToken: token
  })

  return tursoClient
}

// Helper to write wait times to TursoDB
export async function writeWaitTimesToTurso(records: any[]) {
  const turso = getTursoClient()
  let inserted = 0

  for (const record of records) {
    try {
      // Convert types for SQLite
      const data = {
        id: record.id?.toString() || crypto.randomUUID(),
        ride_id: record.item_id?.toString(),
        park_id: record.park_id?.toString() || null,
        wait_time: record.wait_time || 0,
        is_open: record.is_open !== false ? 1 : 0, // Boolean to integer
        status: record.status || null,
        source: record.source || 'queue_times',
        confidence: record.confidence || 1.0,
        recorded_at: record.recorded_at || new Date().toISOString(),
        created_at: new Date().toISOString()
      }

      const columns = Object.keys(data).join(', ')
      const placeholders = Object.keys(data).map(() => '?').join(', ')
      const values = Object.values(data)

      await turso.execute({
        sql: `INSERT OR REPLACE INTO ride_wait_time_history (${columns}) VALUES (${placeholders})`,
        args: values
      })

      inserted++
    } catch (error) {
      console.error('Error inserting wait time:', error)
    }
  }

  return { inserted, total: records.length }
}

// Helper to write weather data to TursoDB
export async function writeWeatherToTurso(records: any[]) {
  const turso = getTursoClient()
  let inserted = 0

  for (const record of records) {
    try {
      const data = {
        id: record.id?.toString() || crypto.randomUUID(),
        park_id: record.location_id?.toString() || null,
        temperature: record.temperature,
        feels_like: record.feels_like,
        precipitation: record.precipitation,
        humidity: record.humidity,
        wind_speed: record.wind_speed,
        uv_index: record.uv_index,
        weather_code: record.weather_code,
        weather_description: record.weather_type,
        cloud_cover: record.cloud_cover || null,
        visibility: record.visibility || null,
        pressure: record.pressure || null,
        recorded_at: record.recorded_at || new Date().toISOString(),
        source: record.source || 'open_meteo',
        created_at: new Date().toISOString()
      }

      const columns = Object.keys(data).filter(k => data[k] !== undefined).join(', ')
      const values = Object.keys(data).filter(k => data[k] !== undefined).map(k => data[k])
      const placeholders = values.map(() => '?').join(', ')

      await turso.execute({
        sql: `INSERT OR REPLACE INTO park_weather_history (${columns}) VALUES (${placeholders})`,
        args: values
      })

      inserted++
    } catch (error) {
      console.error('Error inserting weather:', error)
    }
  }

  return { inserted, total: records.length }
}

// Helper functions for record counts and verification
export async function getTursoTableStats() {
  const turso = getTursoClient()
  
  const tables = ['ride_wait_time_history', 'park_weather_history', 'park_news', 
                 'prediction_features', 'predictions', 'activity_feed', 'prediction_accuracy']
  
  const stats = {}
  for (const table of tables) {
    try {
      const result = await turso.execute(`SELECT COUNT(*) as count FROM ${table}`)
      stats[table] = result.rows[0]?.count || 0
    } catch (error) {
      stats[table] = 0
    }
  }
  
  return stats
}
