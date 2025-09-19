#!/usr/bin/env python3
"""
SINGLE FASTAPI ENDPOINT: GET /score/{apn}
Property scoring API for DealGenie real estate analysis - Production Version
Connected to zimas_unified.db with indexed columns for optimal performance
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import sqlite3
import time
from typing import Dict, Any, Optional
from pathlib import Path
import uvicorn

app = FastAPI(
    title="DealGenie Property API - Production",
    description="Real-time property data lookup using LA County ZIMAS database",
    version="2.0.0"
)

# Database configuration - Updated to real production database
DB_PATH = "scraper/zimas_unified.db"

class PropertyScoreResponse:
    """Response model for property data from zimas_unified.db"""
    def __init__(self, data: Dict[str, Any], query_time_ms: float):
        self.apn = data.get('apn')
        self.site_address = data.get('site_address', '').strip() if data.get('site_address') else None
        self.property_type = data.get('property_type')
        self.zoning_code = data.get('zoning_code')
        self.latitude = data.get('latitude')
        self.longitude = data.get('longitude')
        # Handle JSON data if present
        self.raw_json = data.get('extracted_fields_json')
        self.field_count = data.get('field_count')
        self.query_time_ms = query_time_ms

    def to_dict(self) -> Dict[str, Any]:
        response = {
            "apn": self.apn,
            "site_address": self.site_address if self.site_address and self.site_address != '0' else None,
            "property_details": {
                "property_type": self.property_type,
                "zoning_code": self.zoning_code
            },
            "metadata": {
                "query_time_ms": round(self.query_time_ms, 3),
                "has_extracted_data": bool(self.site_address and self.site_address != '0'),
                "field_count": self.field_count
            }
        }
        
        # Only add location if coordinates are present
        if self.latitude and self.longitude:
            response["location"] = {
                "latitude": self.latitude,
                "longitude": self.longitude
            }
        
        return response

def get_property_by_apn(apn: str) -> Optional[Dict[str, Any]]:
    """
    Query database for property by APN with timing
    Returns tuple of (data, query_time_ms) or (None, query_time_ms)
    """
    start_time = time.perf_counter()
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        cursor = conn.cursor()
        
        query = """
        SELECT 
            apn, 
            site_address,
            property_type,
            zoning_code,
            latitude,
            longitude,
            field_count,
            LENGTH(extracted_fields_json) as json_length
        FROM unified_property_data 
        WHERE apn = ?
        """
        
        cursor.execute(query, (apn,))
        row = cursor.fetchone()
        
        end_time = time.perf_counter()
        query_time_ms = (end_time - start_time) * 1000
        
        if row:
            # Convert sqlite3.Row to dict
            data = dict(row)
            conn.close()
            return data, query_time_ms
        else:
            conn.close()
            return None, query_time_ms
            
    except Exception as e:
        end_time = time.perf_counter()
        query_time_ms = (end_time - start_time) * 1000
        print(f"Database error: {e}")
        return None, query_time_ms

@app.get("/score/{apn}")
async def get_property_score(apn: str) -> JSONResponse:
    """
    Get property crime score and basic data by APN
    
    Args:
        apn: Assessor Parcel Number (string)
        
    Returns:
        JSON response with property data and scores
        
    Raises:
        HTTPException: 404 if APN not found, 500 for database errors
    """
    
    # Validate APN format (basic validation)
    if not apn or len(apn.strip()) == 0:
        raise HTTPException(status_code=400, detail="APN cannot be empty")
    
    if len(apn) > 50:  # Reasonable limit
        raise HTTPException(status_code=400, detail="APN too long")
    
    # Clean APN input
    clean_apn = apn.strip()
    
    # Query database
    data, query_time_ms = get_property_by_apn(clean_apn)
    
    if data is None:
        raise HTTPException(
            status_code=404, 
            detail={
                "error": "Property not found",
                "apn": clean_apn,
                "query_time_ms": round(query_time_ms, 3)
            }
        )
    
    # Build response
    property_response = PropertyScoreResponse(data, query_time_ms)
    
    return JSONResponse(
        status_code=200,
        content=property_response.to_dict()
    )

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "DealGenie Property Scoring API", "status": "active"}

if __name__ == "__main__":
    # Verify database exists
    if not Path(DB_PATH).exists():
        print(f"❌ Database not found: {DB_PATH}")
        exit(1)
    
    print("🚀 Starting DealGenie Property Scoring API")
    print(f"📊 Database: {DB_PATH}")
    
    # Run server
    uvicorn.run(
        app, 
        host="127.0.0.1", 
        port=8000,
        log_level="info"
    )