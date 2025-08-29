#!/usr/bin/env node

/**
 * Test script to verify dual API functionality
 * Tests both Queue-Times and ThemeParks.wiki APIs for a sample park
 */

import { themeParksWiki } from './lib/themeparks-wiki'
import { aggregator } from './lib/aggregator'
import * as fs from 'fs'

// Load mappings
const mappings = JSON.parse(fs.readFileSync('./mappings.json', 'utf-8'))

async function testAPIs() {
  console.log('🧪 Testing Dual API Integration\n')
  console.log('=' .repeat(50))
  
  // Test park: Magic Kingdom
  const testPark = mappings.parks.find((p: any) => p.name === 'Magic Kingdom')
  
  if (!testPark) {
    console.error('❌ Magic Kingdom not found in mappings')
    process.exit(1)
  }
  
  console.log(`\n📍 Testing: ${testPark.name}`)
  console.log(`   Queue-Times ID: ${testPark.queue_times_id}`)
  console.log(`   ThemeParks.wiki ID: ${testPark.themeparks_id}`)
  console.log('-'.repeat(50))
  
  try {
    // Test Queue-Times API
    console.log('\n1️⃣ Testing Queue-Times API...')
    const queueTimesResponse = await fetch(
      `https://queue-times.com/parks/${testPark.queue_times_id}/queue_times.json`
    )
    
    if (queueTimesResponse.ok) {
      const queueData = await queueTimesResponse.json()
      const rideCount = (queueData.rides?.length || 0) + 
                       (queueData.lands?.reduce((sum: number, land: any) => 
                         sum + (land.rides?.length || 0), 0) || 0)
      console.log(`   ✅ Success! Found ${rideCount} rides`)
      
      // Show sample rides
      const sampleRides = queueData.rides?.slice(0, 3) || []
      sampleRides.forEach((ride: any) => {
        console.log(`      - ${ride.name}: ${ride.wait_time} min (${ride.is_open ? 'Open' : 'Closed'})`)
      })
    } else {
      console.log(`   ❌ Failed with status: ${queueTimesResponse.status}`)
    }
    
    // Test ThemeParks.wiki API
    console.log('\n2️⃣ Testing ThemeParks.wiki API...')
    const themeparksData = await themeParksWiki.getParkWaitTimes(testPark.themeparks_id)
    
    if (themeparksData && themeparksData.length > 0) {
      console.log(`   ✅ Success! Found ${themeparksData.length} attractions`)
      
      // Show sample attractions
      const converted = themeparksData
        .map(a => themeParksWiki.convertToSimpleFormat(a))
        .filter(Boolean)
        .slice(0, 3)
      
      converted.forEach((ride: any) => {
        console.log(`      - ${ride.name}: ${ride.wait_time} min (${ride.is_open ? 'Open' : 'Closed'})`)
      })
    } else {
      console.log('   ❌ No data returned or API error')
    }
    
    // Test Aggregation
    console.log('\n3️⃣ Testing Aggregation Logic...')
    
    // Simulate some test data
    const testQueueData = [
      { id: 1, name: 'Space Mountain', wait_time: 45, is_open: true },
      { id: 2, name: 'Thunder Mountain', wait_time: 30, is_open: true },
      { id: 3, name: 'Splash Mountain', wait_time: 0, is_open: false }
    ]
    
    const testThemeparksData = [
      { id: 'abc', name: 'Space Mountain', wait_time: 50, is_open: true },
      { id: 'def', name: 'Thunder Mountain', wait_time: 25, is_open: true },
      { id: 'ghi', name: 'Pirates', wait_time: 15, is_open: true }
    ]
    
    // Simple mapping for test
    const testRideMap = new Map([
      ['1', 'abc'],
      ['2', 'def']
    ])
    
    const aggregatedData = aggregator.processRideData(
      testQueueData,
      testThemeparksData,
      testRideMap
    )
    
    console.log(`   ✅ Processed ${aggregatedData.length} rides:`)
    aggregatedData.forEach(ride => {
      const level = aggregator.getConfidenceLevel(ride.confidenceScore)
      console.log(`      - ${ride.rideName}:`)
      console.log(`        Wait: ${ride.aggregatedWait} min`)
      console.log(`        Confidence: ${(ride.confidenceScore * 100).toFixed(0)}% (${level.level})`)
      if (ride.queueTimesWait !== undefined && ride.themeparksWait !== undefined) {
        console.log(`        Sources: QT=${ride.queueTimesWait}, TP=${ride.themeparksWait}`)
      }
    })
    
    // Test other parks
    console.log('\n4️⃣ Quick test of other mapped parks:')
    const otherParks = mappings.parks.slice(1, 4)
    
    for (const park of otherParks) {
      process.stdout.write(`   - ${park.name}... `)
      
      try {
        const [qtResponse, tpData] = await Promise.all([
          fetch(`https://queue-times.com/parks/${park.queue_times_id}/queue_times.json`),
          themeParksWiki.getParkWaitTimes(park.themeparks_id)
        ])
        
        const qtOk = qtResponse.ok
        const tpOk = tpData && tpData.length > 0
        
        if (qtOk && tpOk) {
          console.log('✅ Both APIs working')
        } else if (qtOk) {
          console.log('⚠️  Only Queue-Times working')
        } else if (tpOk) {
          console.log('⚠️  Only ThemeParks.wiki working')
        } else {
          console.log('❌ Both APIs failed')
        }
      } catch (error) {
        console.log('❌ Error testing')
      }
    }
    
    console.log('\n' + '='.repeat(50))
    console.log('✨ Test complete!')
    console.log('\nNext steps:')
    console.log('1. Run the enhanced collector: npx tsx enhanced-collector.ts')
    console.log('2. Check GitHub Actions: https://github.com/HJSTheJoker/park-metrics-collector/actions')
    console.log('3. Monitor the queue accuracy dashboard in Parkfolio')
    
  } catch (error) {
    console.error('\n❌ Test failed:', error)
    process.exit(1)
  }
}

// Run tests
testAPIs()