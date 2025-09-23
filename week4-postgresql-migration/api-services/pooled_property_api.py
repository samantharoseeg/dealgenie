#!/usr/bin/env python3
"""
Connection Pooled Property API with Visualization Links
Production-ready API with psycopg2 connection pooling and external map visualization
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
from psycopg2 import pool, extras
import json
import time
import threading
import urllib.parse
from typing import Dict, Any, List, Optional
from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os

# Load environment variables (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, use system environment variables only
    pass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class VisualizationLinks:
    """Container for property visualization links"""
    google_maps: str
    google_earth: str
    openstreetmap: str
    apple_maps: str

def validate_coordinates(latitude: float, longitude: float) -> None:
    """Validate coordinates are within LA County bounds"""
    # LA County approximate bounds
    LA_BOUNDS = {
        "lat_min": 33.7,  # Southern boundary
        "lat_max": 34.8,  # Northern boundary
        "lon_min": -119.0, # Western boundary
        "lon_max": -117.6  # Eastern boundary
    }

    if not (LA_BOUNDS["lat_min"] <= latitude <= LA_BOUNDS["lat_max"]):
        raise HTTPException(
            status_code=400,
            detail=f"Latitude {latitude} outside LA County bounds ({LA_BOUNDS['lat_min']}, {LA_BOUNDS['lat_max']})"
        )

    if not (LA_BOUNDS["lon_min"] <= longitude <= LA_BOUNDS["lon_max"]):
        raise HTTPException(
            status_code=400,
            detail=f"Longitude {longitude} outside LA County bounds ({LA_BOUNDS['lon_min']}, {LA_BOUNDS['lon_max']})"
        )

def validate_apn(apn: str) -> None:
    """Validate APN format and prevent SQL injection"""
    if not apn or len(apn.strip()) == 0:
        raise HTTPException(status_code=400, detail="APN cannot be empty")

    # Basic APN format validation - should be alphanumeric
    if not apn.replace("-", "").replace(" ", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid APN format")

    if len(apn) > 20:  # Reasonable APN length limit
        raise HTTPException(status_code=400, detail="APN too long")

def generate_visualization_links(latitude: float, longitude: float, address: str = None) -> Dict[str, str]:
    """Generate external visualization links for a property"""

    # Google Maps with satellite view
    if address:
        query = urllib.parse.quote(address)
        google_maps = f"https://www.google.com/maps/search/{query}/@{latitude},{longitude},18z/data=!3m1!1e3"
    else:
        google_maps = f"https://www.google.com/maps/@{latitude},{longitude},18z/data=!3m1!1e3"

    # Google Earth Web
    google_earth = f"https://earth.google.com/web/@{latitude},{longitude},150a,1000d,35y,0h,0t,0r"

    # OpenStreetMap
    openstreetmap = f"https://www.openstreetmap.org/?mlat={latitude}&mlon={longitude}&zoom=18"

    # Apple Maps (works on iOS/macOS)
    apple_maps = f"https://maps.apple.com/?ll={latitude},{longitude}&z=18&t=s"

    return {
        "google_maps": google_maps,
        "google_earth": google_earth,
        "openstreetmap": openstreetmap,
        "apple_maps": apple_maps
    }

app = FastAPI(
    title="DealGenie Pooled Property API with Visualization",
    version="5.0.0",
    description="Production API with connection pooling and external map visualization links"
)

# CORS middleware - restrict to necessary origins in production
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:8080").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Database configuration with environment variable fallbacks
DATABASE_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "dealgenie_production"),
    "user": os.getenv("DB_USER", "dealgenie_app"),
    "password": os.getenv("DB_PASSWORD", "dealgenie2025"),
    "port": int(os.getenv("DB_PORT", "5432"))
}

# Connection pool configuration
POOL_CONFIG = {
    "minconn": 5,        # Minimum connections
    "maxconn": 20,       # Maximum connections
    "dsn": f"host={DATABASE_CONFIG['host']} "
           f"dbname={DATABASE_CONFIG['database']} "
           f"user={DATABASE_CONFIG['user']} "
           f"password={DATABASE_CONFIG['password']} "
           f"port={DATABASE_CONFIG['port']}"
}

# Global connection pool
connection_pool = None
pool_stats = {
    "total_connections": 0,
    "active_connections": 0,
    "idle_connections": 0,
    "queries_executed": 0,
    "pool_hits": 0,
    "pool_misses": 0
}
pool_lock = threading.Lock()

def initialize_connection_pool():
    """Initialize the PostgreSQL connection pool"""
    global connection_pool

    try:
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            POOL_CONFIG["minconn"],
            POOL_CONFIG["maxconn"],
            **DATABASE_CONFIG
        )

        logger.info(f"Connection pool initialized: min={POOL_CONFIG['minconn']}, max={POOL_CONFIG['maxconn']}")

        # Update initial stats
        with pool_lock:
            pool_stats["total_connections"] = POOL_CONFIG["maxconn"]
            pool_stats["idle_connections"] = POOL_CONFIG["minconn"]

        return connection_pool
    except Exception as e:
        logger.error(f"Failed to create connection pool: {e}")
        raise

def close_connection_pool():
    """Close all connections in the pool"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        logger.info("Connection pool closed")

@contextmanager
def get_db_connection():
    """Context manager for getting database connections from pool"""
    conn = None
    try:
        with pool_lock:
            pool_stats["pool_hits"] += 1

        # Get connection from pool
        conn = connection_pool.getconn()

        if conn:
            with pool_lock:
                pool_stats["active_connections"] += 1
                pool_stats["idle_connections"] = max(0, pool_stats["idle_connections"] - 1)

            yield conn
        else:
            with pool_lock:
                pool_stats["pool_misses"] += 1
            raise Exception("Failed to get connection from pool")

    except Exception as e:
        if conn:
            # Return broken connection
            connection_pool.putconn(conn, close=True)
        raise e
    finally:
        if conn:
            # Return connection to pool
            connection_pool.putconn(conn)

            with pool_lock:
                pool_stats["active_connections"] = max(0, pool_stats["active_connections"] - 1)
                pool_stats["idle_connections"] += 1
                pool_stats["queries_executed"] += 1

def get_pool_statistics():
    """Get current connection pool statistics"""
    with pool_lock:
        stats = pool_stats.copy()

    # Add real-time pool info if available
    try:
        if hasattr(connection_pool, '_pool'):
            stats["pool_size"] = len(connection_pool._pool)
        if hasattr(connection_pool, '_used'):
            stats["connections_in_use"] = len(connection_pool._used)
    except:
        pass

    return stats

# Initialize connection pool on startup
initialize_connection_pool()

@app.on_event("shutdown")
def shutdown_event():
    """Close connection pool on shutdown"""
    close_connection_pool()

@app.get("/health")
def health_check():
    """Health check with connection pool status"""
    start_time = time.time()

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Quick connectivity test
            cursor.execute("SELECT 1")

            # Get basic stats
            cursor.execute("SELECT COUNT(*) FROM unified_property_data")
            total_properties = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL")
            spatial_properties = cursor.fetchone()[0]

            query_time = (time.time() - start_time) * 1000

            return {
                "status": "healthy",
                "database": "PostgreSQL connected via pool",
                "total_properties": total_properties,
                "spatial_properties": spatial_properties,
                "spatial_coverage": f"{(spatial_properties/total_properties*100):.2f}%",
                "query_time_ms": round(query_time, 3),
                "connection_pool": get_pool_statistics()
            }

    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "connection_pool": get_pool_statistics()
            }
        )

@app.get("/api/property/lookup/{apn}")
def lookup_property(apn: str):
    """Lookup property by APN using connection pool"""
    start_time = time.time()

    # Validate APN input
    validate_apn(apn)

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            query = """
            SELECT
                apn,
                site_address,
                total_assessed_value,
                assessed_value_per_sqft,
                zoning_code,
                building_square_footage_numeric,
                year_built_numeric,
                ST_X(geom) as longitude,
                ST_Y(geom) as latitude,
                building_class,
                assessed_land_val_numeric,
                assessed_improvement_val_numeric
            FROM unified_property_data
            WHERE apn = %s
            """

            cursor.execute(query, (apn,))
            row = cursor.fetchone()
            query_time = (time.time() - start_time) * 1000

            if not row:
                raise HTTPException(status_code=404, detail=f"Property with APN {apn} not found")

            # Generate visualization links if coordinates are available
            visualization_links = None
            if row['longitude'] and row['latitude']:
                visualization_links = generate_visualization_links(
                    row['latitude'],
                    row['longitude'],
                    row['site_address']
                )

            return {
                "property": {
                    "apn": row['apn'],
                    "address": row['site_address'] or "Unknown",
                    "assessed_value": float(row['total_assessed_value']) if row['total_assessed_value'] else 0,
                    "value_per_sqft": float(row['assessed_value_per_sqft']) if row['assessed_value_per_sqft'] else 0,
                    "zoning": row['zoning_code'] or "Unknown",
                    "building_sqft": int(row['building_square_footage_numeric']) if row['building_square_footage_numeric'] else 0,
                    "year_built": int(row['year_built_numeric']) if row['year_built_numeric'] else 0,
                    "coordinates": [row['longitude'], row['latitude']] if row['longitude'] and row['latitude'] else None,
                    "building_class": row['building_class'] or "Unknown",
                    "land_value": float(row['assessed_land_val_numeric']) if row['assessed_land_val_numeric'] else 0,
                    "improvement_value": float(row['assessed_improvement_val_numeric']) if row['assessed_improvement_val_numeric'] else 0
                },
                "visualization_links": visualization_links,
                "query_time_ms": round(query_time, 3),
                "connection_pool_stats": get_pool_statistics()
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Property lookup error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/search/nearby")
def search_nearby(
    lat: float = Query(..., description="Latitude coordinate"),
    lon: float = Query(..., description="Longitude coordinate"),
    radius_meters: Optional[int] = Query(1000, description="Search radius in meters"),
    min_value: Optional[int] = Query(None, description="Minimum assessed value"),
    max_value: Optional[int] = Query(None, description="Maximum assessed value"),
    zoning: Optional[str] = Query(None, description="Zoning code filter"),
    limit: Optional[int] = Query(50, description="Maximum results")
):
    """Optimized spatial search using connection pool and bounding box pre-filtering"""
    start_time = time.time()

    # Validate inputs
    if not (-90 <= lat <= 90):
        raise HTTPException(status_code=422, detail="Latitude must be between -90 and 90")
    if not (-180 <= lon <= 180):
        raise HTTPException(status_code=422, detail="Longitude must be between -180 and 180")

    # Enhanced validation for LA County bounds
    validate_coordinates(lat, lon)

    if radius_meters and radius_meters > 10000:
        raise HTTPException(status_code=422, detail="Radius cannot exceed 10km")
    if limit and limit > 100:
        raise HTTPException(status_code=422, detail="Limit cannot exceed 100")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            # Calculate bounding box for pre-filtering
            lat_offset = radius_meters / 111000  # ~111km per degree latitude
            lon_offset = radius_meters / (111000 * abs(lat * 3.14159 / 180))

            # Build optimized query with bounding box pre-filter
            base_query = """
            SELECT
                apn,
                site_address,
                total_assessed_value,
                zoning_code,
                ST_X(geom) as longitude,
                ST_Y(geom) as latitude,
                ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance_meters
            FROM unified_property_data
            WHERE geom IS NOT NULL
            AND longitude BETWEEN %s AND %s
            AND latitude BETWEEN %s AND %s
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
            """

            params = [lon, lat, lon - lon_offset, lon + lon_offset, lat - lat_offset, lat + lat_offset, lon, lat, radius_meters]

            # Add filters
            if min_value:
                base_query += " AND total_assessed_value >= %s"
                params.append(min_value)

            if max_value:
                base_query += " AND total_assessed_value <= %s"
                params.append(max_value)

            if zoning:
                base_query += " AND zoning_code ILIKE %s"
                params.append(f"%{zoning}%")

            base_query += " ORDER BY distance_meters ASC LIMIT %s"
            params.append(limit)

            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            query_time = (time.time() - start_time) * 1000

            properties = []
            for row in rows:
                # Generate visualization links for each property
                visualization_links = None
                if row['longitude'] and row['latitude']:
                    visualization_links = generate_visualization_links(
                        row['latitude'],
                        row['longitude'],
                        row['site_address']
                    )

                properties.append({
                    "apn": row['apn'],
                    "address": f"Property {row['apn']}" if not row['site_address'] else row['site_address'],
                    "assessed_value": float(row['total_assessed_value']) if row['total_assessed_value'] else 0,
                    "zoning": row['zoning_code'] or "Unknown",
                    "coordinates": [row['longitude'], row['latitude']],
                    "distance_meters": round(row['distance_meters'], 1),
                    "visualization_links": visualization_links
                })

            return {
                "nearby_properties": properties,
                "search_center": {"lat": lat, "lon": lon},
                "radius_meters": radius_meters,
                "total_found": len(properties),
                "query_time_ms": round(query_time, 3),
                "connection_pool_stats": get_pool_statistics()
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Spatial search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/api/search/assemblage")
def search_assemblage(
    min_value: Optional[int] = Query(1000000, description="Minimum assessed value"),
    radius_meters: Optional[int] = Query(200, description="Nearby property radius"),
    limit: Optional[int] = Query(20, description="Maximum results")
):
    """Fast assemblage detection using materialized view and connection pool"""
    start_time = time.time()

    if limit and limit > 50:
        raise HTTPException(status_code=422, detail="Limit cannot exceed 50")

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

            query = """
            SELECT
                apn,
                site_address,
                total_assessed_value,
                zoning_code,
                nearby_count,
                longitude,
                latitude
            FROM simple_assemblage
            WHERE total_assessed_value >= %s
            ORDER BY nearby_count DESC, total_assessed_value DESC
            LIMIT %s
            """

            cursor.execute(query, (min_value, limit))
            rows = cursor.fetchall()
            query_time = (time.time() - start_time) * 1000

            opportunities = []
            for row in rows:
                opportunities.append({
                    "apn": row['apn'],
                    "address": row['site_address'] or f"Property {row['apn']}",
                    "assessed_value": float(row['total_assessed_value']) if row['total_assessed_value'] else 0,
                    "zoning": row['zoning_code'] or "Unknown",
                    "nearby_high_value_count": row['nearby_count'],
                    "coordinates": [row['longitude'], row['latitude']] if row['longitude'] and row['latitude'] else None,
                    "assemblage_score": min(100, 50 + (row['nearby_count'] * 5))
                })

            return {
                "assemblage_opportunities": opportunities,
                "total_found": len(opportunities),
                "query_time_ms": round(query_time, 3),
                "connection_pool_stats": get_pool_statistics()
            }

    except Exception as e:
        logger.error(f"Assemblage search error: {e}")
        raise HTTPException(status_code=500, detail=f"Assemblage search error: {str(e)}")

@app.get("/api/pool/stats")
def get_connection_pool_detailed_stats():
    """Get detailed connection pool statistics and configuration"""
    return {
        "pool_stats": get_pool_statistics(),
        "pool_config": POOL_CONFIG,
        "database_config": {k: v if k != 'password' else '***' for k, v in DATABASE_CONFIG.items()}
    }

if __name__ == "__main__":
    import uvicorn

    # Run with optimized settings for connection pooling
    uvicorn.run(
        "pooled_property_api:app",
        host="0.0.0.0",
        port=8005,
        workers=1,  # Single worker to share connection pool
        access_log=True,
        log_level="info"
    )