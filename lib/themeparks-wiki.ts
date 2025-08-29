/**
 * ThemeParks.wiki API Client
 * Free API for theme park wait times and schedules
 * Documentation: https://api.themeparks.wiki/
 */

interface ThemeParksAttraction {
  id: string
  name: string
  entityType: 'ATTRACTION' | 'SHOW' | 'RESTAURANT'
  parkId: string
  status: 'OPERATING' | 'DOWN' | 'CLOSED' | 'REFURBISHMENT'
  queue?: {
    STANDBY?: {
      waitTime: number | null
    }
    SINGLE_RIDER?: {
      waitTime: number | null
    }
    RETURN_TIME?: {
      state: string
      returnStart?: string
      returnEnd?: string
    }
  }
  lastUpdate?: string
}

export class ThemeParksWikiClient {
  private baseUrl = 'https://api.themeparks.wiki/v1'
  private cache = new Map<string, { data: any; timestamp: number }>()
  private cacheDuration = 5 * 60 * 1000 // 5 minutes

  /**
   * Get live wait times for a park
   */
  async getParkWaitTimes(parkId: string): Promise<ThemeParksAttraction[] | null> {
    const cacheKey = `waittimes:${parkId}`
    const cached = this.cache.get(cacheKey)
    
    if (cached && Date.now() - cached.timestamp < this.cacheDuration) {
      return cached.data
    }

    try {
      // Try the live endpoint first
      const response = await fetch(`${this.baseUrl}/entity/${parkId}/live`)
      
      if (!response.ok) {
        if (response.status === 404) {
          // Try alternative endpoint structure
          const altResponse = await fetch(`${this.baseUrl}/entity/${parkId}/children`)
          if (altResponse.ok) {
            const data = await altResponse.json()
            const attractions = (data.children || []).filter(
              (child: any) => child.entityType === 'ATTRACTION' || child.entityType === 'SHOW'
            )
            this.cache.set(cacheKey, { data: attractions, timestamp: Date.now() })
            return attractions
          }
        }
        return null
      }

      const data = await response.json()
      const attractions = data.liveData || []
      
      this.cache.set(cacheKey, { data: attractions, timestamp: Date.now() })
      return attractions
    } catch (error) {
      console.error(`Error fetching wait times for ${parkId}:`, error)
      return null
    }
  }

  /**
   * Convert to simple format for processing
   */
  convertToSimpleFormat(attraction: ThemeParksAttraction): {
    id: string
    name: string
    wait_time: number
    is_open: boolean
    single_rider_time?: number
  } | null {
    const waitTime = attraction.queue?.STANDBY?.waitTime
    const singleRiderTime = attraction.queue?.SINGLE_RIDER?.waitTime
    
    // Skip if no valid data
    if (waitTime === null && singleRiderTime === null && attraction.status !== 'OPERATING') {
      return null
    }
    
    return {
      id: attraction.id,
      name: attraction.name,
      wait_time: waitTime || 0,
      is_open: attraction.status === 'OPERATING',
      single_rider_time: singleRiderTime || undefined
    }
  }

  /**
   * Clear cache
   */
  clearCache(): void {
    this.cache.clear()
  }
}

export const themeParksWiki = new ThemeParksWikiClient()