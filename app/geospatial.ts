/**
 * Geospatial utilities for Redis geohash encoding/decoding and distance calculations
 */

/**
 * Encodes longitude and latitude coordinates into a geohash score
 * @param longitude - Longitude coordinate (-180 to 180)
 * @param latitude - Latitude coordinate (-85.05112878 to 85.05112878)
 * @returns Geohash as a number
 */
export function encodeGeohash(longitude: number, latitude: number): number {
  let lonMin = -180.0;
  let lonMax = 180.0;
  let latMin = -85.05112878;
  let latMax = 85.05112878;
  let geohash = 0;
  let isEven = true;

  for (let i = 0; i < 52; i++) {
    if (isEven) {
      const mid = (lonMin + lonMax) / 2;

      if (longitude > mid) {
        geohash = geohash * 2 + 1;
        lonMin = mid;
      } else {
        geohash = geohash * 2;
        lonMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;

      if (latitude > mid) {
        geohash = geohash * 2 + 1;
        latMin = mid;
      } else {
        geohash = geohash * 2;
        latMax = mid;
      }
    }

    isEven = !isEven;
  }

  return geohash;
}

/**
 * Decodes a geohash score back to longitude and latitude coordinates
 * @param geohash - Geohash score to decode
 * @returns Tuple of [longitude, latitude]
 */
export function decodeGeohash(geohash: number): [number, number] {
  let lonMin = -180.0;
  let lonMax = 180.0;
  let latMin = -85.05112878;
  let latMax = 85.05112878;
  let isEven = true;

  for (let i = 51; i >= 0; i--) {
    const bit = Math.floor(geohash / Math.pow(2, i)) % 2;

    if (isEven) {
      const mid = (lonMin + lonMax) / 2;

      if (bit === 1) {
        lonMin = mid;
      } else {
        lonMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;

      if (bit === 1) {
        latMin = mid;
      } else {
        latMax = mid;
      }
    }

    isEven = !isEven;
  }

  const longitude = (lonMin + lonMax) / 2;
  const latitude = (latMin + latMax) / 2;

  return [longitude, latitude];
}

/**
 * Calculates distance between two coordinates using Haversine formula
 * @param lon1 - First point longitude
 * @param lat1 - First point latitude
 * @param lon2 - Second point longitude
 * @param lat2 - Second point latitude
 * @returns Distance in meters
 */
export function calculateDistance(lon1: number, lat1: number, lon2: number, lat2: number): number {
  const EARTH_RADIUS = 6372797.560856;
  const lat1Rad = lat1 * Math.PI / 180;
  const lat2Rad = lat2 * Math.PI / 180;
  const deltaLatRad = (lat2 - lat1) * Math.PI / 180;
  const deltaLonRad = (lon2 - lon1) * Math.PI / 180;

  const a = Math.sin(deltaLatRad / 2) * Math.sin(deltaLatRad / 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) *
    Math.sin(deltaLonRad / 2) * Math.sin(deltaLonRad / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return EARTH_RADIUS * c;
}
