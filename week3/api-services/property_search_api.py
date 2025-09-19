#!/usr/bin/env python3
"""
Property Search API - Geographic and filtered property search using search_idx_parcel
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import time
from typing import List, Dict, Any, Optional

app = FastAPI(title="DealGenie Property Search API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SEARCH_DB = "search_idx_parcel.db"

@app.get("/")
async def root():
    return {
        "message": "DealGenie Property Search API",
        "version": "1.0.0",
        "features": [
            "Geographic bounding box search",
            "Crime score filtering",
            "Property type filtering", 
            "Address search",
            "Proximity search"
        ]
    }

@app.get("/search/geographic")
async def search_geographic(
    min_lat: float = Query(..., description="Minimum latitude"),
    max_lat: float = Query(..., description="Maximum latitude"),
    min_lon: float = Query(..., description="Minimum longitude"),
    max_lon: float = Query(..., description="Maximum longitude"),
    max_crime: Optional[float] = Query(None, description="Maximum crime score"),
    property_type: Optional[str] = Query(None, description="Property type filter"),
    limit: int = Query(50, description="Maximum results", le=1000)
):
    """Search properties within geographic bounding box"""
    start_time = time.time()
    
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        
        # Build query
        query = """
        SELECT apn, site_address, latitude, longitude, crime_score, crime_tier, 
               property_type, zoning_code
        FROM search_idx_parcel
        WHERE latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        """
        params = [min_lat, max_lat, min_lon, max_lon]
        
        if max_crime is not None:
            query += " AND crime_score <= ?"
            params.append(max_crime)
        
        if property_type:
            query += " AND property_type LIKE ?"
            params.append(f"%{property_type}%")
        
        query += f" LIMIT {limit}"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        query_time = (time.time() - start_time) * 1000
        
        properties = []
        for row in results:
            properties.append({
                "apn": row[0],
                "address": row[1],
                "latitude": row[2],
                "longitude": row[3],
                "crime_score": row[4],
                "crime_tier": row[5],
                "property_type": row[6],
                "zoning_code": row[7]
            })
        
        return {
            "properties": properties,
            "count": len(properties),
            "search_bounds": {
                "min_lat": min_lat,
                "max_lat": max_lat,
                "min_lon": min_lon,
                "max_lon": max_lon
            },
            "filters": {
                "max_crime": max_crime,
                "property_type": property_type
            },
            "query_time_ms": round(query_time, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/search/address")
async def search_address(
    address: str = Query(..., description="Address to search for"),
    limit: int = Query(10, description="Maximum results", le=100)
):
    """Search properties by address"""
    start_time = time.time()
    
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        
        search_pattern = f"%{address.upper()}%"
        
        query = """
        SELECT apn, site_address, latitude, longitude, crime_score, crime_tier,
               property_type, zoning_code
        FROM search_idx_parcel
        WHERE site_address LIKE ?
          AND site_address IS NOT NULL
        ORDER BY 
          CASE 
            WHEN site_address = ? THEN 1
            WHEN site_address LIKE ? THEN 2
            ELSE 3
          END
        LIMIT ?
        """
        
        exact_match = address.upper()
        starts_with = f"{address.upper()}%"
        
        cursor.execute(query, [search_pattern, exact_match, starts_with, limit])
        results = cursor.fetchall()
        conn.close()
        
        query_time = (time.time() - start_time) * 1000
        
        properties = []
        for row in results:
            properties.append({
                "apn": row[0],
                "address": row[1],
                "latitude": row[2],
                "longitude": row[3],
                "crime_score": row[4],
                "crime_tier": row[5],
                "property_type": row[6],
                "zoning_code": row[7]
            })
        
        return {
            "properties": properties,
            "count": len(properties),
            "search_term": address,
            "query_time_ms": round(query_time, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/search/crime")
async def search_by_crime(
    max_score: float = Query(..., description="Maximum crime score"),
    tier: Optional[str] = Query(None, description="Crime tier (Very Low, Low, Moderate, High, Very High)"),
    limit: int = Query(50, description="Maximum results", le=1000)
):
    """Search properties by crime score/tier"""
    start_time = time.time()
    
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        
        query = """
        SELECT apn, site_address, latitude, longitude, crime_score, crime_tier,
               property_type, zoning_code
        FROM search_idx_parcel
        WHERE crime_score <= ?
        """
        params = [max_score]
        
        if tier:
            query += " AND crime_tier = ?"
            params.append(tier)
        
        query += " ORDER BY crime_score ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        query_time = (time.time() - start_time) * 1000
        
        properties = []
        for row in results:
            properties.append({
                "apn": row[0],
                "address": row[1],
                "latitude": row[2],
                "longitude": row[3],
                "crime_score": row[4],
                "crime_tier": row[5],
                "property_type": row[6],
                "zoning_code": row[7]
            })
        
        return {
            "properties": properties,
            "count": len(properties),
            "filters": {
                "max_score": max_score,
                "tier": tier
            },
            "query_time_ms": round(query_time, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/search/proximity")
async def search_proximity(
    lat: float = Query(..., description="Center latitude"),
    lon: float = Query(..., description="Center longitude"),
    radius: float = Query(0.01, description="Search radius in degrees (~1km)", le=0.1),
    max_crime: Optional[float] = Query(None, description="Maximum crime score"),
    limit: int = Query(20, description="Maximum results", le=100)
):
    """Search properties near a specific location"""
    start_time = time.time()
    
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        
        query = """
        SELECT apn, site_address, latitude, longitude, crime_score, crime_tier,
               property_type, zoning_code,
               ABS(latitude - ?) + ABS(longitude - ?) as distance
        FROM search_idx_parcel
        WHERE latitude BETWEEN ? - ? AND ? + ?
          AND longitude BETWEEN ? - ? AND ? + ?
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        """
        params = [lat, lon, lat, radius, lat, radius, lon, radius, lon, radius]
        
        if max_crime is not None:
            query += " AND crime_score <= ?"
            params.append(max_crime)
        
        query += " ORDER BY distance ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        query_time = (time.time() - start_time) * 1000
        
        properties = []
        for row in results:
            properties.append({
                "apn": row[0],
                "address": row[1],
                "latitude": row[2],
                "longitude": row[3],
                "crime_score": row[4],
                "crime_tier": row[5],
                "property_type": row[6],
                "zoning_code": row[7],
                "distance": row[8]
            })
        
        return {
            "properties": properties,
            "count": len(properties),
            "search_center": {"lat": lat, "lon": lon},
            "radius_degrees": radius,
            "filters": {
                "max_crime": max_crime
            },
            "query_time_ms": round(query_time, 3)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/stats")
async def get_statistics():
    """Get search database statistics"""
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        
        # Basic counts
        cursor.execute("SELECT COUNT(*) FROM search_idx_parcel")
        total_properties = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM search_idx_parcel WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        geo_properties = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM search_idx_parcel WHERE site_address IS NOT NULL")
        address_properties = cursor.fetchone()[0]
        
        # Crime statistics
        cursor.execute("SELECT AVG(crime_score), MIN(crime_score), MAX(crime_score) FROM search_idx_parcel")
        avg_crime, min_crime, max_crime = cursor.fetchone()
        
        # Crime tier distribution
        cursor.execute("""
        SELECT crime_tier, COUNT(*) 
        FROM search_idx_parcel 
        GROUP BY crime_tier 
        ORDER BY COUNT(*) DESC
        """)
        crime_tiers = dict(cursor.fetchall())
        
        # Property type distribution (top 10)
        cursor.execute("""
        SELECT property_type, COUNT(*) 
        FROM search_idx_parcel 
        WHERE property_type IS NOT NULL
        GROUP BY property_type 
        ORDER BY COUNT(*) DESC 
        LIMIT 10
        """)
        property_types = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            "total_properties": total_properties,
            "properties_with_coordinates": geo_properties,
            "properties_with_addresses": address_properties,
            "crime_statistics": {
                "average": round(avg_crime, 2),
                "minimum": round(min_crime, 2),
                "maximum": round(max_crime, 2),
                "tier_distribution": crime_tiers
            },
            "top_property_types": property_types,
            "coverage": {
                "geographic": f"{geo_properties/total_properties*100:.1f}%",
                "addresses": f"{address_properties/total_properties*100:.1f}%"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = sqlite3.connect(SEARCH_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM search_idx_parcel LIMIT 1")
        count = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "total_properties": count,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": time.time()
        }

if __name__ == "__main__":
    import uvicorn
    print("🔍 Starting DealGenie Property Search API")
    print("📊 Database: search_idx_parcel.db")
    print("🌐 Server: http://localhost:8002")
    print("📋 Docs: http://localhost:8002/docs")
    print("🎯 Features: Geographic search, Crime filtering, Address lookup")
    
    uvicorn.run(app, host="0.0.0.0", port=8002)