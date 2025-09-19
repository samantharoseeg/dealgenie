#!/usr/bin/env python3
"""
User Data Import System
Assessment of CSV import capabilities, portfolio tracking, and batch analysis
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import sqlite3
import pandas as pd
import json
import csv
import io
from datetime import datetime
import uuid
import re
import requests

app = FastAPI(title="User Data Import Assessment System", version="1.0.0")

SEARCH_DB = "search_idx_parcel.db"
USER_PORTFOLIO_DB = "user_portfolios.db"

# Data Models
class UserProperty(BaseModel):
    address: str
    apn: Optional[str] = None
    notes: Optional[str] = None
    purchase_price: Optional[float] = None
    purchase_date: Optional[str] = None
    property_type: Optional[str] = None
    portfolio_name: Optional[str] = "Default Portfolio"

class Portfolio(BaseModel):
    portfolio_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    portfolio_name: str
    description: Optional[str] = None
    created_date: str = Field(default_factory=lambda: datetime.now().isoformat())
    properties: List[UserProperty] = []

class BatchAnalysisRequest(BaseModel):
    addresses: List[str]
    analysis_type: str = "comprehensive"  # comprehensive, crime_only, financial_only
    include_intelligence: bool = True

class ValidationResult(BaseModel):
    is_valid: bool
    geocoded_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    matched_apn: Optional[str] = None
    confidence_score: float = 0.0
    validation_issues: List[str] = []

def init_user_portfolio_db():
    """Initialize user portfolio database"""
    try:
        conn = sqlite3.connect(USER_PORTFOLIO_DB)
        cursor = conn.cursor()
        
        # Create portfolios table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolios (
                portfolio_id TEXT PRIMARY KEY,
                portfolio_name TEXT NOT NULL,
                description TEXT,
                created_date TEXT,
                updated_date TEXT
            )
        ''')
        
        # Create user_properties table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_properties (
                property_id TEXT PRIMARY KEY,
                portfolio_id TEXT,
                address TEXT NOT NULL,
                apn TEXT,
                notes TEXT,
                purchase_price REAL,
                purchase_date TEXT,
                property_type TEXT,
                geocoded_address TEXT,
                latitude REAL,
                longitude REAL,
                validation_status TEXT,
                intelligence_data TEXT,
                created_date TEXT,
                updated_date TEXT,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios (portfolio_id)
            )
        ''')
        
        # Create import_sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_sessions (
                session_id TEXT PRIMARY KEY,
                portfolio_id TEXT,
                import_type TEXT,
                total_records INTEGER,
                successful_imports INTEGER,
                failed_imports INTEGER,
                validation_results TEXT,
                created_date TEXT,
                FOREIGN KEY (portfolio_id) REFERENCES portfolios (portfolio_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Database initialization error: {str(e)}")

def get_db_connection():
    """Get database connection for property data"""
    try:
        conn = sqlite3.connect(SEARCH_DB)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def get_portfolio_db_connection():
    """Get database connection for user portfolios"""
    try:
        conn = sqlite3.connect(USER_PORTFOLIO_DB)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Portfolio database connection error: {str(e)}")

def validate_and_geocode_address(address: str) -> ValidationResult:
    """Validate and geocode user-provided address"""
    
    validation_result = ValidationResult(is_valid=False)
    validation_issues = []
    
    # Basic address validation
    if not address or len(address.strip()) < 5:
        validation_issues.append("Address too short or empty")
        validation_result.validation_issues = validation_issues
        return validation_result
    
    # Check for basic address components
    has_number = bool(re.search(r'\d+', address))
    has_street = bool(re.search(r'(st|street|ave|avenue|blvd|boulevard|rd|road|way|dr|drive|ln|lane|ct|court)', address.lower()))
    
    if not has_number:
        validation_issues.append("No street number found")
    if not has_street:
        validation_issues.append("No street type indicator found")
    
    # Try to match against existing database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First try exact match
        cursor.execute("""
            SELECT apn, site_address, latitude, longitude, crime_score, zoning_code
            FROM search_idx_parcel 
            WHERE UPPER(site_address) = UPPER(?)
        """, (address.strip(),))
        
        exact_match = cursor.fetchone()
        
        if exact_match:
            validation_result.is_valid = True
            validation_result.matched_apn = exact_match['apn']
            validation_result.geocoded_address = exact_match['site_address']
            validation_result.latitude = exact_match['latitude']
            validation_result.longitude = exact_match['longitude']
            validation_result.confidence_score = 1.0
            validation_issues.append("Exact match found in database")
        else:
            # Try fuzzy match on address components
            address_parts = address.upper().split()
            if len(address_parts) >= 2:
                street_number = address_parts[0] if address_parts[0].isdigit() else None
                
                if street_number:
                    # Look for properties on same street
                    cursor.execute("""
                        SELECT apn, site_address, latitude, longitude, crime_score
                        FROM search_idx_parcel 
                        WHERE UPPER(site_address) LIKE ?
                        LIMIT 5
                    """, (f"%{' '.join(address_parts[1:3])}%",))
                    
                    similar_matches = cursor.fetchall()
                    if similar_matches:
                        # Use first match as approximation
                        best_match = similar_matches[0]
                        validation_result.is_valid = True
                        validation_result.geocoded_address = f"Near {best_match['site_address']}"
                        validation_result.latitude = best_match['latitude']
                        validation_result.longitude = best_match['longitude']
                        validation_result.confidence_score = 0.7
                        validation_issues.append(f"Approximate match found (similar street)")
                    else:
                        validation_issues.append("No similar addresses found in database")
            
            if not validation_result.is_valid:
                # Basic geocoding simulation based on LA patterns
                if any(indicator in address.upper() for indicator in ['HOLLYWOOD', 'VENICE', 'SANTA MONICA', 'BEVERLY', 'DOWNTOWN']):
                    validation_result.is_valid = True
                    validation_result.geocoded_address = f"Geocoded: {address}"
                    # Simulate LA coordinates
                    validation_result.latitude = 34.0522 + (hash(address) % 100) / 1000
                    validation_result.longitude = -118.2437 + (hash(address) % 100) / 1000
                    validation_result.confidence_score = 0.5
                    validation_issues.append("Geocoded using external service simulation")
                else:
                    validation_issues.append("Could not geocode address")
        
        conn.close()
        
    except Exception as e:
        validation_issues.append(f"Database lookup error: {str(e)}")
    
    validation_result.validation_issues = validation_issues
    return validation_result

def enrich_property_with_intelligence(property_data: Dict, lat: float, lng: float) -> Dict:
    """Enrich user property with intelligence data from existing systems"""
    
    enriched_data = property_data.copy()
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find nearest properties for intelligence data
        cursor.execute("""
            SELECT apn, site_address, latitude, longitude, crime_score, crime_tier,
                   property_type, zoning_code, year_built, sqft, data_quality
            FROM search_idx_parcel
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY 
                ABS(latitude - ?) + ABS(longitude - ?)
            LIMIT 5
        """, (lat, lng))
        
        nearby_properties = cursor.fetchall()
        
        if nearby_properties:
            # Use nearest property data for intelligence enrichment
            nearest = nearby_properties[0]
            
            enriched_data.update({
                'intelligence_data': {
                    'crime_analysis': {
                        'area_crime_score': nearest['crime_score'],
                        'crime_tier': nearest['crime_tier'],
                        'neighborhood_safety': 'High' if nearest['crime_score'] < 30 else 'Moderate' if nearest['crime_score'] < 60 else 'Low'
                    },
                    'market_analysis': {
                        'area_property_type': nearest['property_type'],
                        'area_zoning': nearest['zoning_code'],
                        'typical_year_built': nearest['year_built'],
                        'area_sqft_average': nearest['sqft']
                    },
                    'data_quality': {
                        'intelligence_score': nearest['data_quality'],
                        'nearby_properties_analyzed': len(nearby_properties),
                        'data_source': 'LA County Property Database'
                    },
                    'location_intelligence': {
                        'coordinates': {'lat': lat, 'lng': lng},
                        'distance_to_downtown': abs(lat - 34.0522) + abs(lng + 118.2437),
                        'neighborhood_properties': len(nearby_properties)
                    }
                }
            })
        
        conn.close()
        
    except Exception as e:
        enriched_data['intelligence_error'] = str(e)
    
    return enriched_data

# API Endpoints
@app.get("/")
async def root():
    """User data import system overview"""
    return {
        "message": "User Data Import Assessment System",
        "version": "1.0.0",
        "capabilities": [
            "CSV property list import",
            "Portfolio creation and management",
            "Address validation and geocoding",
            "Intelligence system integration",
            "Batch property analysis",
            "Data quality assessment"
        ],
        "supported_formats": ["CSV", "JSON", "Manual entry"],
        "endpoints": [
            "/import/csv - CSV file upload and processing",
            "/portfolio/create - Create new property portfolio",
            "/portfolio/{portfolio_id} - Portfolio management",
            "/batch/analyze - Batch analysis of address lists",
            "/validate/address - Address validation and geocoding",
            "/import/interface - User import interface"
        ]
    }

@app.get("/import/interface", response_class=HTMLResponse)
async def import_interface():
    """User data import interface"""
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Property Data Import System</title>
        <style>
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: #333; min-height: 100vh;
            }
            .container { 
                max-width: 1200px; margin: 0 auto; background: white; 
                padding: 30px; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            }
            .header {
                text-align: center; margin-bottom: 40px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; padding: 30px; border-radius: 10px; margin: -30px -30px 40px -30px;
            }
            .import-section {
                margin: 30px 0; padding: 25px; background: #f8f9ff; 
                border-radius: 12px; border: 2px solid #e1e5e9;
            }
            .section-title {
                font-size: 20px; font-weight: 600; margin-bottom: 15px; 
                color: #667eea; display: flex; align-items: center;
            }
            .section-icon {
                width: 24px; height: 24px; margin-right: 10px; 
                background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 50%;
            }
            .upload-area {
                border: 3px dashed #667eea; border-radius: 10px; padding: 40px; 
                text-align: center; background: white; margin: 20px 0;
                transition: all 0.3s ease;
            }
            .upload-area:hover {
                border-color: #764ba2; background: #f0f4ff;
            }
            .upload-area.dragover {
                border-color: #764ba2; background: #e6f0ff;
            }
            .btn {
                padding: 12px 24px; background: linear-gradient(135deg, #667eea, #764ba2); 
                color: white; border: none; border-radius: 8px; cursor: pointer; 
                font-weight: 500; transition: all 0.3s ease; margin: 5px;
            }
            .btn:hover {
                transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
            }
            .input-group {
                margin: 15px 0; display: flex; align-items: center; gap: 10px;
            }
            .input-group label {
                min-width: 120px; font-weight: 500; color: #555;
            }
            .input-group input, .input-group select, .input-group textarea {
                flex: 1; padding: 10px; border: 1px solid #ddd; border-radius: 6px;
            }
            .results-area {
                margin: 20px 0; padding: 20px; background: white; border-radius: 8px;
                border: 1px solid #ddd; min-height: 100px; max-height: 400px; overflow-y: auto;
            }
            .sample-csv {
                background: #f4f4f4; padding: 15px; border-radius: 8px; margin: 15px 0;
                font-family: 'Courier New', monospace; font-size: 12px;
            }
            .status-indicator {
                display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px;
            }
            .status-success { background: #4caf50; }
            .status-warning { background: #ff9800; }
            .status-error { background: #f44336; }
            .portfolio-grid {
                display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px;
            }
            .portfolio-card {
                background: white; padding: 20px; border-radius: 10px; border: 1px solid #ddd;
                transition: all 0.3s ease;
            }
            .portfolio-card:hover {
                box-shadow: 0 5px 15px rgba(0,0,0,0.1); transform: translateY(-2px);
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Property Data Import System</h1>
                <p>Import your property lists, create portfolios, and leverage AI intelligence</p>
                <p>CSV upload • Portfolio tracking • Batch analysis • Intelligence integration</p>
            </div>

            <!-- CSV Import Section -->
            <div class="import-section">
                <div class="section-title">
                    <div class="section-icon"></div>
                    📋 CSV Property List Import
                </div>
                
                <div class="sample-csv">
                    <strong>Sample CSV Format:</strong><br>
                    address,apn,notes,purchase_price,purchase_date,property_type<br>
                    "123 Main St, Los Angeles CA","5551-002-025","Great location",500000,"2023-01-15","Residential"<br>
                    "456 Hollywood Blvd","","Investment property",750000,"2023-02-20","Commercial"
                </div>
                
                <div class="upload-area" id="uploadArea">
                    <div style="font-size: 48px; margin-bottom: 15px;">📁</div>
                    <div style="font-size: 18px; margin-bottom: 10px;">Drop CSV file here or click to browse</div>
                    <div style="color: #666;">Supports: .csv, .xlsx (max 10MB)</div>
                    <input type="file" id="csvFile" accept=".csv,.xlsx" style="display: none;">
                    <button class="btn" onclick="document.getElementById('csvFile').click()">Choose File</button>
                </div>
                
                <div class="input-group">
                    <label>Portfolio Name:</label>
                    <input type="text" id="portfolioName" placeholder="e.g., My Investment Properties" value="Imported Properties">
                </div>
                
                <button class="btn" onclick="processCsvImport()">Process Import</button>
                
                <div class="results-area" id="importResults" style="display: none;">
                    <div id="importStatus"></div>
                </div>
            </div>

            <!-- Manual Property Entry -->
            <div class="import-section">
                <div class="section-title">
                    <div class="section-icon"></div>
                    ✏️ Manual Property Entry
                </div>
                
                <div class="input-group">
                    <label>Address:</label>
                    <input type="text" id="manualAddress" placeholder="e.g., 123 Main St, Los Angeles CA">
                </div>
                
                <div class="input-group">
                    <label>APN (optional):</label>
                    <input type="text" id="manualApn" placeholder="e.g., 5551-002-025">
                </div>
                
                <div class="input-group">
                    <label>Notes:</label>
                    <textarea id="manualNotes" placeholder="Property notes, investment strategy, etc." rows="3"></textarea>
                </div>
                
                <div class="input-group">
                    <label>Purchase Price:</label>
                    <input type="number" id="manualPrice" placeholder="500000">
                </div>
                
                <div class="input-group">
                    <label>Property Type:</label>
                    <select id="manualType">
                        <option value="">Select type</option>
                        <option value="Residential">Residential</option>
                        <option value="Commercial">Commercial</option>
                        <option value="Multi-Family">Multi-Family</option>
                        <option value="Land">Land</option>
                        <option value="Mixed-Use">Mixed-Use</option>
                    </select>
                </div>
                
                <button class="btn" onclick="addManualProperty()">Add Property</button>
                <button class="btn" onclick="validateAddress()">Validate Address Only</button>
                
                <div class="results-area" id="manualResults" style="display: none;">
                    <div id="validationStatus"></div>
                </div>
            </div>

            <!-- Batch Address Analysis -->
            <div class="import-section">
                <div class="section-title">
                    <div class="section-icon"></div>
                    🔍 Batch Address Analysis
                </div>
                
                <div class="input-group">
                    <label>Address List:</label>
                    <textarea id="batchAddresses" placeholder="Enter one address per line:
123 Main St, Los Angeles CA
456 Hollywood Blvd
789 Sunset Strip, West Hollywood CA" rows="5"></textarea>
                </div>
                
                <div class="input-group">
                    <label>Analysis Type:</label>
                    <select id="analysisType">
                        <option value="comprehensive">Comprehensive Analysis</option>
                        <option value="crime_only">Crime Analysis Only</option>
                        <option value="financial_only">Financial Analysis Only</option>
                        <option value="validation_only">Address Validation Only</option>
                    </select>
                </div>
                
                <button class="btn" onclick="runBatchAnalysis()">Run Batch Analysis</button>
                
                <div class="results-area" id="batchResults" style="display: none;">
                    <div id="batchStatus"></div>
                </div>
            </div>

            <!-- Portfolio Management -->
            <div class="import-section">
                <div class="section-title">
                    <div class="section-icon"></div>
                    📁 Portfolio Management
                </div>
                
                <button class="btn" onclick="loadPortfolios()">Load My Portfolios</button>
                <button class="btn" onclick="createNewPortfolio()">Create New Portfolio</button>
                
                <div class="portfolio-grid" id="portfolioGrid" style="display: none;">
                    <!-- Portfolios will be loaded here -->
                </div>
            </div>
        </div>

        <script>
            // File upload handling
            const uploadArea = document.getElementById('uploadArea');
            const csvFile = document.getElementById('csvFile');

            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('dragover');
            });

            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('dragover');
            });

            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');
                const files = e.dataTransfer.files;
                if (files.length > 0) {
                    csvFile.files = files;
                    processCsvImport();
                }
            });

            function processCsvImport() {
                const file = csvFile.files[0];
                const portfolioName = document.getElementById('portfolioName').value;
                
                if (!file) {
                    alert('Please select a CSV file first.');
                    return;
                }

                const formData = new FormData();
                formData.append('file', file);
                formData.append('portfolio_name', portfolioName);

                document.getElementById('importResults').style.display = 'block';
                document.getElementById('importStatus').innerHTML = 'Processing CSV file...';

                fetch('/import/csv', {
                    method: 'POST',
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    displayImportResults(data);
                })
                .catch(error => {
                    document.getElementById('importStatus').innerHTML = `<span class="status-indicator status-error"></span>Error: ${error.message}`;
                });
            }

            function displayImportResults(data) {
                let html = `<h4>Import Results</h4>`;
                html += `<p><span class="status-indicator status-success"></span>Portfolio: ${data.portfolio_name}</p>`;
                html += `<p><span class="status-indicator status-success"></span>Processed: ${data.total_processed} properties</p>`;
                html += `<p><span class="status-indicator status-success"></span>Successful: ${data.successful_imports}</p>`;
                
                if (data.failed_imports > 0) {
                    html += `<p><span class="status-indicator status-error"></span>Failed: ${data.failed_imports}</p>`;
                }

                if (data.properties && data.properties.length > 0) {
                    html += `<h5>Imported Properties:</h5><ul>`;
                    data.properties.slice(0, 5).forEach(prop => {
                        const status = prop.validation_result?.is_valid ? 'success' : 'warning';
                        html += `<li><span class="status-indicator status-${status}"></span>${prop.address}`;
                        if (prop.intelligence_data) {
                            html += ` - Crime Score: ${prop.intelligence_data.crime_analysis?.area_crime_score || 'N/A'}`;
                        }
                        html += `</li>`;
                    });
                    html += `</ul>`;
                }

                document.getElementById('importStatus').innerHTML = html;
            }

            function addManualProperty() {
                const propertyData = {
                    address: document.getElementById('manualAddress').value,
                    apn: document.getElementById('manualApn').value,
                    notes: document.getElementById('manualNotes').value,
                    purchase_price: parseFloat(document.getElementById('manualPrice').value) || null,
                    property_type: document.getElementById('manualType').value
                };

                if (!propertyData.address) {
                    alert('Please enter an address.');
                    return;
                }

                document.getElementById('manualResults').style.display = 'block';
                document.getElementById('validationStatus').innerHTML = 'Adding property...';

                fetch('/property/add', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(propertyData)
                })
                .then(response => response.json())
                .then(data => {
                    displayValidationResults(data);
                })
                .catch(error => {
                    document.getElementById('validationStatus').innerHTML = `<span class="status-indicator status-error"></span>Error: ${error.message}`;
                });
            }

            function validateAddress() {
                const address = document.getElementById('manualAddress').value;
                
                if (!address) {
                    alert('Please enter an address.');
                    return;
                }

                document.getElementById('manualResults').style.display = 'block';
                document.getElementById('validationStatus').innerHTML = 'Validating address...';

                fetch(`/validate/address?address=${encodeURIComponent(address)}`)
                .then(response => response.json())
                .then(data => {
                    displayValidationResults(data);
                })
                .catch(error => {
                    document.getElementById('validationStatus').innerHTML = `<span class="status-indicator status-error"></span>Error: ${error.message}`;
                });
            }

            function displayValidationResults(data) {
                let html = `<h4>Validation Results</h4>`;
                
                if (data.is_valid) {
                    html += `<p><span class="status-indicator status-success"></span>Address validated successfully</p>`;
                    if (data.geocoded_address) {
                        html += `<p><strong>Geocoded:</strong> ${data.geocoded_address}</p>`;
                    }
                    if (data.latitude && data.longitude) {
                        html += `<p><strong>Coordinates:</strong> ${data.latitude}, ${data.longitude}</p>`;
                    }
                    html += `<p><strong>Confidence:</strong> ${(data.confidence_score * 100).toFixed(0)}%</p>`;
                } else {
                    html += `<p><span class="status-indicator status-error"></span>Address validation failed</p>`;
                }

                if (data.validation_issues && data.validation_issues.length > 0) {
                    html += `<p><strong>Issues:</strong></p><ul>`;
                    data.validation_issues.forEach(issue => {
                        html += `<li>${issue}</li>`;
                    });
                    html += `</ul>`;
                }

                document.getElementById('validationStatus').innerHTML = html;
            }

            function runBatchAnalysis() {
                const addresses = document.getElementById('batchAddresses').value.trim().split('\\n').filter(addr => addr.trim());
                const analysisType = document.getElementById('analysisType').value;

                if (addresses.length === 0) {
                    alert('Please enter at least one address.');
                    return;
                }

                document.getElementById('batchResults').style.display = 'block';
                document.getElementById('batchStatus').innerHTML = `Analyzing ${addresses.length} addresses...`;

                fetch('/batch/analyze', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        addresses: addresses,
                        analysis_type: analysisType,
                        include_intelligence: analysisType !== 'validation_only'
                    })
                })
                .then(response => response.json())
                .then(data => {
                    displayBatchResults(data);
                })
                .catch(error => {
                    document.getElementById('batchStatus').innerHTML = `<span class="status-indicator status-error"></span>Error: ${error.message}`;
                });
            }

            function displayBatchResults(data) {
                let html = `<h4>Batch Analysis Results</h4>`;
                html += `<p><span class="status-indicator status-success"></span>Analyzed: ${data.total_addresses} addresses</p>`;
                html += `<p><span class="status-indicator status-success"></span>Valid: ${data.valid_addresses}</p>`;
                html += `<p><span class="status-indicator status-warning"></span>Invalid: ${data.invalid_addresses}</p>`;

                if (data.results && data.results.length > 0) {
                    html += `<h5>Results:</h5><ul>`;
                    data.results.forEach(result => {
                        const status = result.validation_result?.is_valid ? 'success' : 'error';
                        html += `<li><span class="status-indicator status-${status}"></span>${result.address}`;
                        if (result.intelligence_data && result.intelligence_data.crime_analysis) {
                            html += ` - Crime: ${result.intelligence_data.crime_analysis.area_crime_score}`;
                        }
                        html += `</li>`;
                    });
                    html += `</ul>`;
                }

                document.getElementById('batchStatus').innerHTML = html;
            }

            function loadPortfolios() {
                document.getElementById('portfolioGrid').style.display = 'block';
                
                fetch('/portfolio/list')
                .then(response => response.json())
                .then(data => {
                    displayPortfolios(data.portfolios || []);
                })
                .catch(error => {
                    console.error('Error loading portfolios:', error);
                });
            }

            function displayPortfolios(portfolios) {
                const grid = document.getElementById('portfolioGrid');
                
                if (portfolios.length === 0) {
                    grid.innerHTML = '<p>No portfolios found. Create your first portfolio!</p>';
                    return;
                }

                let html = '';
                portfolios.forEach(portfolio => {
                    html += `
                        <div class="portfolio-card">
                            <h4>${portfolio.portfolio_name}</h4>
                            <p>${portfolio.description || 'No description'}</p>
                            <p><strong>Properties:</strong> ${portfolio.property_count || 0}</p>
                            <p><strong>Created:</strong> ${new Date(portfolio.created_date).toLocaleDateString()}</p>
                            <button class="btn" onclick="viewPortfolio('${portfolio.portfolio_id}')">View Details</button>
                        </div>
                    `;
                });
                
                grid.innerHTML = html;
            }

            function createNewPortfolio() {
                const name = prompt('Enter portfolio name:');
                if (name) {
                    fetch('/portfolio/create', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({
                            portfolio_name: name,
                            description: 'Created via import interface'
                        })
                    })
                    .then(response => response.json())
                    .then(data => {
                        alert(`Portfolio "${data.portfolio_name}" created successfully!`);
                        loadPortfolios();
                    })
                    .catch(error => {
                        alert(`Error creating portfolio: ${error.message}`);
                    });
                }
            }

            function viewPortfolio(portfolioId) {
                window.open(`/portfolio/${portfolioId}`, '_blank');
            }
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

@app.post("/import/csv")
async def import_csv(file: UploadFile = File(...), portfolio_name: str = Form("Imported Properties")):
    """Import property list from CSV file"""
    
    if not file.filename.endswith(('.csv', '.xlsx')):
        raise HTTPException(status_code=400, detail="File must be CSV or Excel format")
    
    try:
        # Read file content
        content = await file.read()
        
        # Parse CSV data
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(content.decode('utf-8')))
        else:
            df = pd.read_excel(io.BytesIO(content))
        
        # Validate required columns
        required_columns = ['address']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {missing_columns}")
        
        # Initialize portfolio database
        init_user_portfolio_db()
        
        # Create portfolio
        portfolio_id = str(uuid.uuid4())
        conn = get_portfolio_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO portfolios (portfolio_id, portfolio_name, description, created_date, updated_date)
            VALUES (?, ?, ?, ?, ?)
        """, (portfolio_id, portfolio_name, f"Imported from {file.filename}", 
              datetime.now().isoformat(), datetime.now().isoformat()))
        
        # Process each property
        properties = []
        successful_imports = 0
        failed_imports = 0
        
        for index, row in df.iterrows():
            try:
                address = str(row['address']).strip()
                if not address or address.lower() in ['nan', 'null', '']:
                    continue
                
                # Validate and geocode address
                validation_result = validate_and_geocode_address(address)
                
                # Create property data
                property_data = {
                    'address': address,
                    'apn': str(row.get('apn', '')),
                    'notes': str(row.get('notes', '')),
                    'purchase_price': float(row['purchase_price']) if pd.notna(row.get('purchase_price')) else None,
                    'purchase_date': str(row.get('purchase_date', '')),
                    'property_type': str(row.get('property_type', '')),
                    'validation_result': validation_result.dict()
                }
                
                # Enrich with intelligence data if validated
                if validation_result.is_valid and validation_result.latitude and validation_result.longitude:
                    enriched_data = enrich_property_with_intelligence(
                        property_data, validation_result.latitude, validation_result.longitude
                    )
                    property_data.update(enriched_data)
                
                # Save to database
                property_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO user_properties 
                    (property_id, portfolio_id, address, apn, notes, purchase_price, 
                     purchase_date, property_type, geocoded_address, latitude, longitude,
                     validation_status, intelligence_data, created_date, updated_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    property_id, portfolio_id, address, property_data['apn'],
                    property_data['notes'], property_data['purchase_price'],
                    property_data['purchase_date'], property_data['property_type'],
                    validation_result.geocoded_address, validation_result.latitude, 
                    validation_result.longitude, 'valid' if validation_result.is_valid else 'invalid',
                    json.dumps(property_data.get('intelligence_data', {})),
                    datetime.now().isoformat(), datetime.now().isoformat()
                ))
                
                properties.append(property_data)
                successful_imports += 1
                
            except Exception as e:
                failed_imports += 1
                print(f"Error processing row {index}: {str(e)}")
        
        # Save import session
        session_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO import_sessions 
            (session_id, portfolio_id, import_type, total_records, successful_imports, 
             failed_imports, validation_results, created_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id, portfolio_id, 'csv_import', len(df),
            successful_imports, failed_imports, json.dumps([]),
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        return {
            "message": "CSV import completed",
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio_name,
            "total_processed": len(df),
            "successful_imports": successful_imports,
            "failed_imports": failed_imports,
            "properties": properties[:10]  # Return first 10 for preview
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import error: {str(e)}")

@app.get("/validate/address")
async def validate_address_endpoint(address: str):
    """Validate and geocode a single address"""
    
    if not address:
        raise HTTPException(status_code=400, detail="Address parameter required")
    
    validation_result = validate_and_geocode_address(address)
    return validation_result

@app.post("/batch/analyze")
async def batch_analyze_addresses(request: BatchAnalysisRequest):
    """Batch analysis of multiple addresses"""
    
    if not request.addresses:
        raise HTTPException(status_code=400, detail="No addresses provided")
    
    results = []
    valid_addresses = 0
    invalid_addresses = 0
    
    for address in request.addresses:
        try:
            # Validate address
            validation_result = validate_and_geocode_address(address.strip())
            
            result = {
                'address': address.strip(),
                'validation_result': validation_result.dict()
            }
            
            # Add intelligence data if requested and validated
            if (request.include_intelligence and validation_result.is_valid and 
                validation_result.latitude and validation_result.longitude):
                
                enriched_data = enrich_property_with_intelligence(
                    {'address': address}, validation_result.latitude, validation_result.longitude
                )
                result.update(enriched_data)
            
            results.append(result)
            
            if validation_result.is_valid:
                valid_addresses += 1
            else:
                invalid_addresses += 1
                
        except Exception as e:
            results.append({
                'address': address.strip(),
                'error': str(e),
                'validation_result': {'is_valid': False, 'validation_issues': [str(e)]}
            })
            invalid_addresses += 1
    
    return {
        "total_addresses": len(request.addresses),
        "valid_addresses": valid_addresses,
        "invalid_addresses": invalid_addresses,
        "analysis_type": request.analysis_type,
        "results": results
    }

@app.post("/portfolio/create")
async def create_portfolio(portfolio: Portfolio):
    """Create a new property portfolio"""
    
    init_user_portfolio_db()
    
    try:
        conn = get_portfolio_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO portfolios (portfolio_id, portfolio_name, description, created_date, updated_date)
            VALUES (?, ?, ?, ?, ?)
        """, (portfolio.portfolio_id, portfolio.portfolio_name, portfolio.description,
              portfolio.created_date, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return {
            "message": "Portfolio created successfully",
            "portfolio_id": portfolio.portfolio_id,
            "portfolio_name": portfolio.portfolio_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Portfolio creation error: {str(e)}")

@app.get("/portfolio/list")
async def list_portfolios():
    """List all user portfolios"""
    
    init_user_portfolio_db()
    
    try:
        conn = get_portfolio_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT p.portfolio_id, p.portfolio_name, p.description, p.created_date,
                   COUNT(up.property_id) as property_count
            FROM portfolios p
            LEFT JOIN user_properties up ON p.portfolio_id = up.portfolio_id
            GROUP BY p.portfolio_id, p.portfolio_name, p.description, p.created_date
            ORDER BY p.created_date DESC
        """)
        
        portfolios = []
        for row in cursor.fetchall():
            portfolios.append({
                'portfolio_id': row['portfolio_id'],
                'portfolio_name': row['portfolio_name'],
                'description': row['description'],
                'created_date': row['created_date'],
                'property_count': row['property_count']
            })
        
        conn.close()
        
        return {"portfolios": portfolios}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Portfolio listing error: {str(e)}")

@app.get("/portfolio/{portfolio_id}")
async def get_portfolio_details(portfolio_id: str):
    """Get detailed portfolio information"""
    
    try:
        conn = get_portfolio_db_connection()
        cursor = conn.cursor()
        
        # Get portfolio info
        cursor.execute("""
            SELECT portfolio_id, portfolio_name, description, created_date
            FROM portfolios WHERE portfolio_id = ?
        """, (portfolio_id,))
        
        portfolio_row = cursor.fetchone()
        if not portfolio_row:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        
        # Get properties
        cursor.execute("""
            SELECT property_id, address, apn, notes, purchase_price, purchase_date,
                   property_type, geocoded_address, latitude, longitude, validation_status,
                   intelligence_data, created_date
            FROM user_properties WHERE portfolio_id = ?
            ORDER BY created_date DESC
        """, (portfolio_id,))
        
        properties = []
        for row in cursor.fetchall():
            prop_data = {
                'property_id': row['property_id'],
                'address': row['address'],
                'apn': row['apn'],
                'notes': row['notes'],
                'purchase_price': row['purchase_price'],
                'purchase_date': row['purchase_date'],
                'property_type': row['property_type'],
                'geocoded_address': row['geocoded_address'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'validation_status': row['validation_status'],
                'created_date': row['created_date']
            }
            
            # Parse intelligence data
            if row['intelligence_data']:
                try:
                    prop_data['intelligence_data'] = json.loads(row['intelligence_data'])
                except:
                    pass
            
            properties.append(prop_data)
        
        conn.close()
        
        portfolio_data = {
            'portfolio_id': portfolio_row['portfolio_id'],
            'portfolio_name': portfolio_row['portfolio_name'],
            'description': portfolio_row['description'],
            'created_date': portfolio_row['created_date'],
            'property_count': len(properties),
            'properties': properties
        }
        
        return portfolio_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Portfolio retrieval error: {str(e)}")

@app.post("/property/add")
async def add_property_manual(property_data: UserProperty):
    """Add a single property manually"""
    
    init_user_portfolio_db()
    
    try:
        # Validate address
        validation_result = validate_and_geocode_address(property_data.address)
        
        # Create or find portfolio
        conn = get_portfolio_db_connection()
        cursor = conn.cursor()
        
        # Check if default portfolio exists
        cursor.execute("""
            SELECT portfolio_id FROM portfolios WHERE portfolio_name = ?
        """, (property_data.portfolio_name,))
        
        portfolio_row = cursor.fetchone()
        if not portfolio_row:
            # Create default portfolio
            portfolio_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO portfolios (portfolio_id, portfolio_name, description, created_date, updated_date)
                VALUES (?, ?, ?, ?, ?)
            """, (portfolio_id, property_data.portfolio_name, "Default portfolio for manual entries",
                  datetime.now().isoformat(), datetime.now().isoformat()))
        else:
            portfolio_id = portfolio_row['portfolio_id']
        
        # Enrich with intelligence if validated
        enriched_data = {}
        if validation_result.is_valid and validation_result.latitude and validation_result.longitude:
            enriched_data = enrich_property_with_intelligence(
                property_data.dict(), validation_result.latitude, validation_result.longitude
            )
        
        # Save property
        property_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO user_properties 
            (property_id, portfolio_id, address, apn, notes, purchase_price, 
             purchase_date, property_type, geocoded_address, latitude, longitude,
             validation_status, intelligence_data, created_date, updated_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            property_id, portfolio_id, property_data.address, property_data.apn,
            property_data.notes, property_data.purchase_price, property_data.purchase_date,
            property_data.property_type, validation_result.geocoded_address,
            validation_result.latitude, validation_result.longitude,
            'valid' if validation_result.is_valid else 'invalid',
            json.dumps(enriched_data.get('intelligence_data', {})),
            datetime.now().isoformat(), datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        result = validation_result.dict()
        result.update({
            'property_id': property_id,
            'portfolio_id': portfolio_id,
            'message': 'Property added successfully'
        })
        
        if enriched_data.get('intelligence_data'):
            result['intelligence_data'] = enriched_data['intelligence_data']
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Property addition error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    init_user_portfolio_db()
    uvicorn.run(app, host="0.0.0.0", port=8011)