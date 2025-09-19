#!/usr/bin/env python3
"""
Address to APN lookup service for the API
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from typing import Optional

app = FastAPI(title="Address to APN Lookup")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/lookup/{address}")
async def lookup_apn(address: str):
    """Lookup APN by address"""
    try:
        conn = sqlite3.connect("scraper/zimas_unified.db")
        cursor = conn.cursor()
        
        # Search for address (case-insensitive, partial match)
        query = """
        SELECT apn, site_address, property_type, zoning_code
        FROM unified_property_data
        WHERE UPPER(site_address) LIKE UPPER(?)
        AND site_address IS NOT NULL
        AND site_address != '0'
        LIMIT 5
        """
        
        search_term = f"%{address.strip()}%"
        cursor.execute(query, (search_term,))
        results = cursor.fetchall()
        conn.close()
        
        if results:
            matches = []
            for row in results:
                matches.append({
                    "apn": row[0],
                    "address": row[1],
                    "property_type": row[2],
                    "zoning_code": row[3]
                })
            
            # Return best match and alternatives
            return {
                "found": True,
                "best_match": matches[0],
                "alternatives": matches[1:] if len(matches) > 1 else []
            }
        else:
            raise HTTPException(status_code=404, detail="Address not found")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)