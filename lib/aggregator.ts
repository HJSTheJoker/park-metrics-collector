/**
 * Wait time aggregation and confidence scoring
 * Intelligently combines data from multiple sources
 */

export interface WaitTimeData {
  rideId: string
  rideName: string
  queueTimesWait?: number
  themeparksWait?: number
  aggregatedWait: number
  confidenceScore: number
  isOpen: boolean
  singleRiderTime?: number
}

export class WaitTimeAggregator {
  /**
   * Calculate confidence score based on data availability and agreement
   */
  private calculateConfidence(queueTime?: number, themeparksTime?: number): number {
    // No data available
    if (queueTime === undefined && themeparksTime === undefined) {
      return 0.0
    }
    
    // Only one source available
    if (queueTime === undefined || themeparksTime === undefined) {
      return 0.5
    }
    
    // Both sources available - check agreement
    const difference = Math.abs(queueTime - themeparksTime)
    const average = (queueTime + themeparksTime) / 2
    
    // Handle closed rides (both report 0)
    if (queueTime === 0 && themeparksTime === 0) {
      return 1.0
    }
    
    // Calculate percentage difference
    if (average === 0) {
      return difference === 0 ? 1.0 : 0.3
    }
    
    const percentDiff = (difference / average) * 100
    
    // Score based on agreement
    if (percentDiff <= 10) {
      return 1.0 // Excellent agreement (within 10%)
    } else if (percentDiff <= 20) {
      return 0.9 // Good agreement (within 20%)
    } else if (percentDiff <= 30) {
      return 0.8 // Fair agreement (within 30%)
    } else if (percentDiff <= 50) {
      return 0.6 // Poor agreement (within 50%)
    } else {
      return 0.4 // Very poor agreement (>50% difference)
    }
  }
  
  /**
   * Aggregate wait times from multiple sources
   */
  aggregateWaitTime(queueTime?: number, themeparksTime?: number): {
    aggregated: number
    confidence: number
  } {
    const confidence = this.calculateConfidence(queueTime, themeparksTime)
    
    // No data
    if (queueTime === undefined && themeparksTime === undefined) {
      return { aggregated: 0, confidence: 0 }
    }
    
    // Single source
    if (queueTime === undefined) {
      return { aggregated: themeparksTime!, confidence }
    }
    if (themeparksTime === undefined) {
      return { aggregated: queueTime, confidence }
    }
    
    // Both sources available - use weighted average based on historical accuracy
    // For now, use simple average with slight preference for Queue-Times
    const queueWeight = 0.55 // Slightly prefer Queue-Times
    const themeparksWeight = 0.45
    
    const aggregated = Math.round(
      (queueTime * queueWeight) + (themeparksTime * themeparksWeight)
    )
    
    return { aggregated, confidence }
  }
  
  /**
   * Process ride data from both sources
   */
  processRideData(
    queueTimesData: any[],
    themeparksData: any[],
    rideMapping: Map<string, string>
  ): WaitTimeData[] {
    const results: WaitTimeData[] = []
    const processedRides = new Set<string>()
    
    // Process Queue-Times data
    for (const ride of queueTimesData) {
      const themeparksId = rideMapping.get(ride.id.toString())
      const themeparksRide = themeparksId 
        ? themeparksData.find(r => r.id === themeparksId)
        : null
      
      const { aggregated, confidence } = this.aggregateWaitTime(
        ride.wait_time,
        themeparksRide?.wait_time
      )
      
      results.push({
        rideId: ride.id.toString(),
        rideName: ride.name,
        queueTimesWait: ride.wait_time,
        themeparksWait: themeparksRide?.wait_time,
        aggregatedWait: aggregated,
        confidenceScore: confidence,
        isOpen: ride.is_open || themeparksRide?.is_open || false,
        singleRiderTime: themeparksRide?.single_rider_time
      })
      
      processedRides.add(ride.id.toString())
      if (themeparksId) {
        processedRides.add(themeparksId)
      }
    }
    
    // Process remaining ThemeParks.wiki data (rides not in Queue-Times)
    for (const ride of themeparksData) {
      if (!processedRides.has(ride.id)) {
        const { aggregated, confidence } = this.aggregateWaitTime(
          undefined,
          ride.wait_time
        )
        
        results.push({
          rideId: ride.id,
          rideName: ride.name,
          queueTimesWait: undefined,
          themeparksWait: ride.wait_time,
          aggregatedWait: aggregated,
          confidenceScore: confidence,
          isOpen: ride.is_open || false,
          singleRiderTime: ride.single_rider_time
        })
      }
    }
    
    return results
  }
  
  /**
   * Get confidence level description
   */
  getConfidenceLevel(score: number): {
    level: 'high' | 'medium' | 'low' | 'none'
    description: string
    icon: string
  } {
    if (score >= 0.8) {
      return {
        level: 'high',
        description: 'High confidence - multiple sources agree',
        icon: '✓'
      }
    } else if (score >= 0.6) {
      return {
        level: 'medium',
        description: 'Medium confidence - sources partially agree',
        icon: '?'
      }
    } else if (score > 0) {
      return {
        level: 'low',
        description: 'Low confidence - limited or conflicting data',
        icon: '⚠'
      }
    } else {
      return {
        level: 'none',
        description: 'No data available',
        icon: '✗'
      }
    }
  }
}

export const aggregator = new WaitTimeAggregator()