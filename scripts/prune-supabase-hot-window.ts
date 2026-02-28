#!/usr/bin/env tsx

import * as fs from 'fs'
import { createClient } from '@supabase/supabase-js'

type TableName = 'ride_wait_time_history' | 'park_weather_history'

type TableResult = {
  table: TableName
  deletedRows: number
  batches: number
  remainingOlderRows: number
  oldestRemainingRecordedAt: string | null
  hitBatchLimit: boolean
}

type SupabaseClientLike = any

function env(name: string): string | undefined {
  const value = process.env[name]
  return value && value.trim().length > 0 ? value.trim() : undefined
}

function requiredEnv(name: string): string {
  const value = env(name)
  if (!value) throw new Error(`Missing required env var: ${name}`)
  return value
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const raw = env(name)
  const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, parsed))
}

async function countOlderRows(
  supabase: SupabaseClientLike,
  table: TableName,
  cutoffIso: string
): Promise<number> {
  const { count, error } = await supabase
    .from(table)
    .select('id', { head: true, count: 'exact' })
    .lt('recorded_at', cutoffIso)

  if (error) throw new Error(`count older rows failed for ${table}: ${error.message}`)
  return Number(count ?? 0)
}

async function oldestRecordedAt(
  supabase: SupabaseClientLike,
  table: TableName
): Promise<string | null> {
  const { data, error } = await supabase
    .from(table)
    .select('recorded_at')
    .order('recorded_at', { ascending: true })
    .limit(1)
    .maybeSingle()

  if (error) throw new Error(`oldest query failed for ${table}: ${error.message}`)
  return data?.recorded_at ?? null
}

async function pruneTableInBatches(
  supabase: SupabaseClientLike,
  table: TableName,
  cutoffIso: string,
  batchSize: number,
  maxBatches: number,
  deleteChunkSize: number
): Promise<TableResult> {
  let deletedRows = 0
  let batches = 0
  let hitBatchLimit = false

  while (true) {
    if (batches >= maxBatches) {
      hitBatchLimit = true
      break
    }

    const { data, error } = await supabase
      .from(table)
      .select('id')
      .lt('recorded_at', cutoffIso)
      .order('recorded_at', { ascending: true })
      .limit(batchSize)

    if (error) throw new Error(`select batch failed for ${table}: ${error.message}`)

    const ids = (data ?? []).map((row: any) => row.id).filter(Boolean)
    if (ids.length === 0) {
      break
    }

    for (let idx = 0; idx < ids.length; idx += deleteChunkSize) {
      const chunk = ids.slice(idx, idx + deleteChunkSize)
      const { count, error: deleteError } = await supabase
        .from(table)
        .delete({ count: 'exact' })
        .in('id', chunk)

      if (deleteError) throw new Error(`delete batch failed for ${table}: ${deleteError.message}`)
      deletedRows += Number(count ?? chunk.length)
    }

    batches += 1

    // Supabase may enforce a practical max rows-per-response lower than batchSize.
    // Keep looping until no rows remain so prune can fully drain old data.
  }

  const remainingOlderRows = await countOlderRows(supabase, table, cutoffIso)
  const oldestRemainingRecordedAt = await oldestRecordedAt(supabase, table)

  return {
    table,
    deletedRows,
    batches,
    remainingOlderRows,
    oldestRemainingRecordedAt,
    hitBatchLimit,
  }
}

async function insertCronLog(
  supabase: SupabaseClientLike,
  status: 'started' | 'completed' | 'failed',
  details: Record<string, any>,
  errorMessage: string | null,
  startedAtMs: number
) {
  try {
    await supabase.from('cron_logs').insert({
      job_name: 'prune-supabase-hot-window',
      status,
      execution_time_ms: Date.now() - startedAtMs,
      error_message: errorMessage,
      details,
    })
  } catch {
    // Best effort only; do not mask prune job outcome.
  }
}

async function main() {
  const startedAtMs = Date.now()
  const startedAtIso = new Date(startedAtMs).toISOString()

  const supabaseUrl = requiredEnv('NEXT_PUBLIC_SUPABASE_URL')
  const serviceRoleKey = requiredEnv('SUPABASE_SERVICE_ROLE_KEY')
  const retentionHours = envInt('SUPABASE_RETENTION_HOURS', 48, 1, 24 * 30)
  const batchSize = envInt('PRUNE_BATCH_SIZE', 5000, 100, 10000)
  const maxBatches = envInt('PRUNE_MAX_BATCHES', 250, 1, 2000)
  const deleteChunkSize = envInt('PRUNE_DELETE_CHUNK_SIZE', 100, 25, 1000)
  const reportFile = env('REPORT_FILE') ?? 'supabase-prune-report.json'

  const cutoffIso = new Date(Date.now() - retentionHours * 60 * 60 * 1000).toISOString()
  const supabase = createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  })

  await insertCronLog(
    supabase,
    'started',
    {
      startedAt: startedAtIso,
      retentionHours,
      cutoffIso,
      batchSize,
      maxBatches,
      deleteChunkSize,
    },
    null,
    startedAtMs
  )

  try {
    const tables: TableName[] = ['ride_wait_time_history', 'park_weather_history']
    const tableResults: TableResult[] = []

    for (const table of tables) {
      const result = await pruneTableInBatches(
        supabase,
        table,
        cutoffIso,
        batchSize,
        maxBatches,
        deleteChunkSize
      )
      tableResults.push(result)
    }

    const totalDeleted = tableResults.reduce((sum, tableResult) => sum + tableResult.deletedRows, 0)
    const hasRemainingOlder = tableResults.some((tableResult) => tableResult.remainingOlderRows > 0)
    const hitBatchLimit = tableResults.some((tableResult) => tableResult.hitBatchLimit)

    const summary = {
      startedAt: startedAtIso,
      finishedAt: new Date().toISOString(),
      executionTimeMs: Date.now() - startedAtMs,
      retentionHours,
      cutoffIso,
      batchSize,
      maxBatches,
      deleteChunkSize,
      totalDeleted,
      tableResults,
      hasRemainingOlder,
      hitBatchLimit,
    }

    fs.writeFileSync(reportFile, JSON.stringify(summary, null, 2), 'utf8')

    if (hasRemainingOlder) {
      await insertCronLog(
        supabase,
        'failed',
        summary,
        'Rows older than retention window remain after prune run',
        startedAtMs
      )
      console.error(JSON.stringify(summary, null, 2))
      throw new Error('Rows older than retention window remain after prune run')
    }

    await insertCronLog(supabase, 'completed', summary, null, startedAtMs)
    console.log(JSON.stringify(summary, null, 2))
  } catch (error: any) {
    const errorMessage = String(error?.message ?? error)
    await insertCronLog(
      supabase,
      'failed',
      {
        startedAt: startedAtIso,
        finishedAt: new Date().toISOString(),
        executionTimeMs: Date.now() - startedAtMs,
        retentionHours,
        cutoffIso,
        batchSize,
        maxBatches,
        deleteChunkSize,
      },
      errorMessage,
      startedAtMs
    )
    throw error
  }
}

main().catch((error) => {
  console.error(error)
  process.exit(1)
})
