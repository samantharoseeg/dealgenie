#!/usr/bin/env python3
"""
LA Building Permits ETL Extractor

Extracts building permit data from LA City's Socrata API with:
- Socrata API authentication with app tokens
- Incremental extraction using updated_at cursors
- Rate limiting with verified quota guards
- Circuit breaker patterns for resilience
- Comprehensive governance tracking
- Staging to Parquet before DB loading
"""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

import logging
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from normalization.address_parser import AddressParser
from geocoding import HierarchicalGeocoder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SocrataAPIConfig:
    """Socrata API authentication and configuration"""
    
    def __init__(self):
        # Socrata API authentication
        self.app_token = os.getenv('SOCRATA_APP_TOKEN')
        self.username = os.getenv('SOCRATA_USERNAME')
        self.password = os.getenv('SOCRATA_PASSWORD')
        
        # LA City specific limits (verified from Socrata documentation)
        # Default limits: 1000 requests/hour without app token, 10,000 with app token
        self.authenticated_limit = 10000 if self.app_token else 1000
        self.throttled_limit = 100  # When throttled
        
        # Safety margins
        self.safety_margin = 0.8  # Use 80% of quota
        self.throttle_safety_margin = 0.5  # Use 50% when throttled
        
        logger.info(f"Socrata API config: authenticated={bool(self.app_token)}, "
                   f"limit={self.authenticated_limit}/hour")
    
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Socrata API requests"""
        headers = {
            'User-Agent': 'DealGenie ETL Pipeline/1.0 (permits-extractor)',
            'Accept': 'application/json'
        }
        
        if self.app_token:
            headers['X-App-Token'] = self.app_token
            logger.debug("Using Socrata app token authentication")
        else:
            logger.warning("No Socrata app token - using reduced rate limits")
        
        return headers
    
    def get_auth(self) -> Optional[Tuple[str, str]]:
        """Get HTTP basic auth credentials if available"""
        if self.username and self.password:
            logger.debug("Using Socrata basic authentication")
            return (self.username, self.password)
        return None
    
    def get_effective_rate_limit(self, is_throttled: bool = False) -> int:
        """Get effective rate limit based on authentication and throttling status"""
        base_limit = self.throttled_limit if is_throttled else self.authenticated_limit
        safety_margin = self.throttle_safety_margin if is_throttled else self.safety_margin
        return int(base_limit * safety_margin)

class RateLimiter:
    """Token bucket rate limiter with Socrata-specific quota management"""
    
    def __init__(self, api_config: SocrataAPIConfig):
        self.api_config = api_config
        self.max_calls = api_config.get_effective_rate_limit()
        self.window_seconds = 3600
        self.calls = []
        self.is_throttled = False
        self.throttle_detected_at = None
        self._lock = asyncio.Lock()
        
        logger.info(f"Rate limiter configured: {self.max_calls} calls/hour")
    
    def _update_throttling_status(self, response_headers: Dict[str, str]):
        """Update throttling status based on API response headers"""
        # Check for Socrata throttling headers
        remaining = response_headers.get('x-ratelimit-remaining')
        limit = response_headers.get('x-ratelimit-limit')
        
        if remaining and limit:
            remaining_int = int(remaining)
            limit_int = int(limit)
            
            # If we're below 10% of limit, consider ourselves throttled
            if remaining_int < (limit_int * 0.1):
                if not self.is_throttled:
                    self.is_throttled = True
                    self.throttle_detected_at = datetime.now()
                    self.max_calls = self.api_config.get_effective_rate_limit(is_throttled=True)
                    logger.warning(f"API throttling detected. Reducing rate to {self.max_calls}/hour")
            else:
                if self.is_throttled:
                    self.is_throttled = False
                    self.max_calls = self.api_config.get_effective_rate_limit(is_throttled=False)
                    logger.info(f"API throttling cleared. Restoring rate to {self.max_calls}/hour")
    
    async def acquire(self):
        """Wait if necessary to respect rate limits"""
        async with self._lock:
            now = time.time()
            # Remove calls outside the window
            self.calls = [t for t in self.calls if now - t < self.window_seconds]
            
            if len(self.calls) >= self.max_calls:
                # Calculate wait time
                oldest_call = min(self.calls)
                wait_time = self.window_seconds - (now - oldest_call) + 1
                logger.warning(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                await asyncio.sleep(wait_time)
                now = time.time()
            
            self.calls.append(now)

class CircuitBreaker:
    """Circuit breaker for API resilience"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
        self._lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self.state == "open":
                if (datetime.now() - self.last_failure_time).seconds > self.recovery_timeout:
                    self.state = "half-open"
                    logger.info("Circuit breaker entering half-open state")
                else:
                    raise Exception("Circuit breaker is open")
            
            try:
                result = await func(*args, **kwargs)
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful call")
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = datetime.now()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
                    logger.error(f"Circuit breaker opened after {self.failure_count} failures")
                
                raise e

class LAPermitsExtractor:
    """LA Building Permits ETL Extractor"""
    
    # LA City Building Permits dataset on Socrata  
    API_BASE = "https://data.lacity.org/resource"
    DATASET_ID = "pi9x-tg5x"  # Building and Safety - Building Permits Issued from 2020 to Present
    
    # Field mappings from Socrata to our schema (updated for pi9x-tg5x dataset)
    FIELD_MAPPINGS = {
        'permit_nbr': 'permit_number',
        'permit_type': 'permit_type', 
        'permit_sub_type': 'permit_subtype',
        'status_desc': 'status',
        'status_date': 'status_date',
        'issue_date': 'issue_date',
        'work_desc': 'work_description',
        'valuation': 'valuation',
        'primary_address': 'primary_address',
        'zip_code': 'zip_code',
        'lat': 'latitude',
        'lon': 'longitude',
        'cd': 'council_district',
        'ct': 'census_tract',
        'zone': 'zoning',
        'apc': 'area_planning_commission',
        'cpa': 'community_plan_area', 
        'cnc': 'neighborhood_council',
        'pin_nbr': 'pin_number',
        'apn': 'assessor_parcel_number',
        'use_code': 'use_code',
        'use_desc': 'use_description',
        'permit_group': 'permit_group',
        'business_unit': 'business_unit',
        'ev': 'electric_vehicle_ready',
        'solar': 'solar_ready',
        'hl': 'hillside_lot'
    }
    
    def __init__(self, db_path: str = "dealgenie.db", staging_dir: str = "data/staging"):
        self.db_path = Path(db_path)
        self.staging_dir = Path(staging_dir)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Socrata API configuration
        self.api_config = SocrataAPIConfig()
        
        # Initialize components with proper authentication
        self.rate_limiter = RateLimiter(api_config=self.api_config)
        self.circuit_breaker = CircuitBreaker()
        self.address_parser = AddressParser()
        self.geocoder = HierarchicalGeocoder(user_agent="DealGenie ETL Pipeline/1.0")
        
        # Track extraction state
        self.extraction_id = None
        self.start_time = None
        self.records_extracted = 0
        self.api_calls_made = 0
        self.quota_usage = {
            'calls_made': 0,
            'quota_remaining': None,
            'quota_limit': None,
            'reset_time': None
        }
        
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        self._ensure_tables_exist(conn)
        return conn
    
    def _ensure_tables_exist(self, conn: sqlite3.Connection):
        """Ensure required tables exist with correct schema"""
        # Create raw_permits table with updated schema for pi9x-tg5x dataset
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_permits (
                natural_key_hash TEXT PRIMARY KEY,
                permit_number TEXT,
                permit_type TEXT,
                permit_subtype TEXT,
                status TEXT,
                status_date TEXT,
                issue_date TEXT,
                work_description TEXT,
                valuation REAL,
                units_added INTEGER,
                primary_address TEXT,
                address_raw TEXT,
                address_normalized TEXT,
                address_components TEXT,
                city TEXT,
                state TEXT,
                zip_code TEXT,
                latitude REAL,
                longitude REAL,
                council_district TEXT,
                census_tract TEXT,
                zoning TEXT,
                area_planning_commission TEXT,
                community_plan_area TEXT,
                neighborhood_council TEXT,
                pin_number TEXT,
                assessor_parcel_number TEXT,
                use_code TEXT,
                use_description TEXT,
                permit_group TEXT,
                business_unit TEXT,
                electric_vehicle_ready TEXT,
                solar_ready TEXT,
                geocode_quality TEXT,
                source_endpoint TEXT,
                query_params TEXT,
                as_of_date TEXT,
                ingest_timestamp TEXT
            )
        """)
        
        # Create etl_audit table for tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS etl_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_name TEXT,
                run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                records_processed INTEGER,
                duration_seconds REAL,
                error_message TEXT,
                metadata TEXT
            )
        """)
        
        conn.commit()
    
    def _get_last_cursor(self) -> Optional[str]:
        """Get last successful extraction cursor from etl_audit"""
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                SELECT metadata
                FROM etl_audit
                WHERE pipeline_name = 'permits_extractor'
                  AND status = 'success'
                ORDER BY run_timestamp DESC
                LIMIT 1
            """)
            row = cursor.fetchone()
            if row and row['metadata']:
                metadata = json.loads(row['metadata'])
                return metadata.get('last_cursor')
            return None
        finally:
            conn.close()
    
    def _log_audit(self, status: str, records_processed: int = 0, 
                   error_message: str = None, metadata: Dict = None):
        """Log extraction run to etl_audit table"""
        conn = self._get_connection()
        try:
            duration_seconds = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            
            audit_metadata = {
                'extraction_id': self.extraction_id,
                'api_calls': self.api_calls_made,
                'dataset_id': self.DATASET_ID,
                'staging_files': [],
                **(metadata or {})
            }
            
            conn.execute("""
                INSERT INTO etl_audit (
                    pipeline_name, run_timestamp, status, 
                    records_processed, duration_seconds,
                    error_message, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'permits_extractor',
                datetime.now().isoformat(),
                status,
                records_processed,
                duration_seconds,
                error_message,
                json.dumps(audit_metadata)
            ))
            conn.commit()
        finally:
            conn.close()
    
    def _construct_address(self, row: Dict) -> str:
        """Construct full address from available fields with improved geocoding compatibility"""
        # For the pi9x-tg5x dataset, primary_address is already provided
        if row.get('primary_address'):
            address = row['primary_address'].strip()
            
            # Clean up problematic address formats
            # Handle unit ranges that break geocoding (e.g., "1-75")
            address = re.sub(r'\s+(\d+)-(\d+)$', r' UNIT \1-\2', address)
            
            # Ensure proper Los Angeles suffix for better geocoding
            if not re.search(r'\bLOS ANGELES\b', address.upper()) and not re.search(r'\bCA\b', address.upper()):
                address += ', Los Angeles, CA'
            
            return address
        
        # Fallback: construct from components (for compatibility)
        parts = []
        
        # Street number range
        if row.get('address_start'):
            if row.get('address_end') and row['address_end'] != row['address_start']:
                parts.append(f"{row['address_start']}-{row['address_end']}")
            else:
                parts.append(str(row['address_start']))
        
        # Street direction
        if row.get('street_direction'):
            parts.append(row['street_direction'])
        
        # Street name
        if row.get('street_name'):
            parts.append(row['street_name'])
        
        # Street suffix
        if row.get('street_suffix'):
            parts.append(row['street_suffix'])
        
        address = ' '.join(parts) if parts else None
        if address:
            address += ', Los Angeles, CA'
            
        return address
    
    def _extract_units_added(self, description: str) -> Optional[int]:
        """Extract number of units from work description with comprehensive patterns"""
        if not description:
            return None
            
        description_upper = description.upper()
        units = 0
        
        # ADU patterns (Accessory Dwelling Unit) - common in LA
        # Check for numbered ADUs first
        adu_number_match = re.search(r'(\d+)\s+(?:DETACHED\s+)?ADU', description_upper)
        if adu_number_match:
            adu_count = int(adu_number_match.group(1))
            units = max(units, adu_count)
        elif any(pattern in description_upper for pattern in [
            'ADU', 'ACESSORY DWELLING', 'ACCESSORY DWELLING', 'GRANNY FLAT', 
            'JUNIOR ADU', 'JADU', 'JUNIOR ACCESSORY', 'JUNIOR ACESSORY'
        ]):
            units += 1
            
        # Multi-family patterns
        if re.search(r'\bDUPLEX\b', description_upper):
            units = max(units, 2)
        if re.search(r'\bTRIPLEX\b', description_upper):
            units = max(units, 3)
        if re.search(r'\bFOURPLEX|4-PLEX\b', description_upper):
            units = max(units, 4)
            
        # Multi-unit patterns with explicit numbers
        unit_patterns = [
            r'(\d+)[\s-]*UNIT[S]?\b',
            r'(\d+)[\s-]*DWELLING[S]?\b', 
            r'(\d+)[\s-]*APARTMENT[S]?\b',
            r'(\d+)[\s-]*FAMILY\b',
            r'BUILDING.*?(\d+)[\s-]*UNIT[S]?\b',
            r'CONTAINING\s+(\d+)\s+(?:DETACHED\s+)?(?:UNIT[S]?|ADU)',
            r'(\d+)\s+DETACHED\s+ADU',
            r'(\d+)\s+EACH\s+UNIT',
            r'REMODEL\s+(\d+)\s+UNIT[S]?',
            r'(\d+)\s+RESIDENTIAL\s+UNIT[S]?'
        ]
        
        for pattern in unit_patterns:
            matches = re.findall(pattern, description_upper)
            if matches:
                numbers = [int(m) for m in matches if m.isdigit()]
                if numbers:
                    units = max(units, max(numbers))
        
        # Single family patterns - only set if no other units found
        if units == 0 and any(pattern in description_upper for pattern in ['SFD', 'SINGLE FAMILY', 'SFR']):
            units = 1
            
        # Convert bedroom/bathroom conversions (common ADU pattern)
        if units == 0 and re.search(r'CONVERT.*?(GARAGE|BASEMENT).*?(BEDROOM|LIVING|APARTMENT)', description_upper):
            units = 1
            
        # Handle unit changes in supplemental permits
        unit_change_match = re.search(r'DWELLING\s+UNITS?\s+FROM\s+["\']?(\d+)["\']?\s+TO\s+["\']?(\d+)["\']?', description_upper)
        if unit_change_match:
            from_units = int(unit_change_match.group(1))
            to_units = int(unit_change_match.group(2))
            # Net change in units
            units = max(units, to_units - from_units)
            
        # Handle "TWO UNITS", "THREE UNITS", etc. written out
        word_numbers = {
            'ONE': 1, 'TWO': 2, 'THREE': 3, 'FOUR': 4, 'FIVE': 5,
            'SIX': 6, 'SEVEN': 7, 'EIGHT': 8, 'NINE': 9, 'TEN': 10
        }
        for word, number in word_numbers.items():
            if re.search(rf'\b{word}\s+UNIT[S]?\b', description_upper):
                units = max(units, number)
                break
                
        # Handle demolition patterns (negative units, but we'll return 0 for demolitions)
        if 'DEMOLITION' in description_upper and units == 0:
            demo_match = re.search(r'DEMOLITION\s+OF\s+(\d+)\s+UNIT', description_upper)
            if demo_match:
                return None  # Don't count demolished units as added
                
        return units if units > 0 else None
    
    def _update_quota_usage(self, response: httpx.Response):
        """Update quota usage tracking from API response headers"""
        headers = response.headers
        
        # Update rate limiter throttling status
        self.rate_limiter._update_throttling_status(dict(headers))
        
        # Track quota usage for monitoring
        if 'x-ratelimit-remaining' in headers:
            self.quota_usage['quota_remaining'] = int(headers['x-ratelimit-remaining'])
        if 'x-ratelimit-limit' in headers:
            self.quota_usage['quota_limit'] = int(headers['x-ratelimit-limit'])
        if 'x-ratelimit-reset' in headers:
            self.quota_usage['reset_time'] = int(headers['x-ratelimit-reset'])
        
        self.quota_usage['calls_made'] = self.api_calls_made
        
        # Log quota status periodically
        if self.api_calls_made % 10 == 0:  # Every 10 calls
            remaining = self.quota_usage.get('quota_remaining', 'unknown')
            limit = self.quota_usage.get('quota_limit', 'unknown')
            logger.info(f"Quota usage: {remaining}/{limit} remaining")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(httpx.HTTPError),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    async def _fetch_page(self, client: httpx.AsyncClient, offset: int, 
                         limit: int = 1000, where_clause: str = None) -> Dict:
        """Fetch a page of permits from Socrata API with authentication"""
        await self.rate_limiter.acquire()
        
        params = {
            '$limit': limit,
            '$offset': offset,
            '$order': 'status_date DESC, permit_nbr',
        }
        
        if where_clause:
            params['$where'] = where_clause
        
        url = f"{self.API_BASE}/{self.DATASET_ID}.json"
        query_string = urlencode(params)
        full_url = f"{url}?{query_string}"
        
        logger.info(f"Fetching page at offset {offset} (limit: {limit})")
        
        async def make_authenticated_request():
            # Get authentication headers and credentials
            headers = self.api_config.get_headers()
            auth = self.api_config.get_auth()
            
            response = await client.get(
                full_url, 
                headers=headers,
                auth=auth,
                timeout=30.0
            )
            
            # Handle authentication errors
            if response.status_code == 401:
                raise httpx.HTTPStatusError(
                    "Authentication failed. Check SOCRATA_APP_TOKEN and credentials.",
                    request=response.request,
                    response=response
                )
            elif response.status_code == 403:
                raise httpx.HTTPStatusError(
                    "Access forbidden. Check API permissions and rate limits.",
                    request=response.request,
                    response=response
                )
            elif response.status_code == 429:
                # Rate limit exceeded
                retry_after = response.headers.get('retry-after', '60')
                logger.warning(f"Rate limit exceeded. Retry after {retry_after}s")
                raise httpx.HTTPStatusError(
                    f"Rate limit exceeded. Retry after {retry_after}s",
                    request=response.request,
                    response=response
                )
            
            response.raise_for_status()
            
            # Update quota usage tracking
            self._update_quota_usage(response)
            
            self.api_calls_made += 1
            return response.json()
        
        return await self.circuit_breaker.call(make_authenticated_request)
    
    async def extract_permits(self, 
                            start_date: Optional[datetime] = None,
                            end_date: Optional[datetime] = None,
                            incremental: bool = True) -> int:
        """
        Extract permits from LA City API
        
        Args:
            start_date: Start of extraction window
            end_date: End of extraction window  
            incremental: Use last cursor for incremental extraction
            
        Returns:
            Number of records extracted
        """
        self.extraction_id = hashlib.md5(f"{datetime.now().isoformat()}".encode()).hexdigest()[:12]
        self.start_time = datetime.now()
        self.records_extracted = 0
        self.api_calls_made = 0
        
        logger.info(f"Starting permits extraction (ID: {self.extraction_id})")
        
        try:
            # Build where clause for date filtering
            where_clauses = []
            
            if incremental:
                last_cursor = self._get_last_cursor()
                if last_cursor:
                    where_clauses.append(f"status_date > '{last_cursor}'")
                    logger.info(f"Using incremental cursor: {last_cursor}")
            
            if start_date:
                where_clauses.append(f"status_date >= '{start_date.isoformat()}'")
            
            if end_date:
                where_clauses.append(f"status_date <= '{end_date.isoformat()}'")
            
            # Default to last 30 days if no constraints
            if not where_clauses:
                thirty_days_ago = (datetime.now() - timedelta(days=30)).isoformat()
                where_clauses.append(f"status_date >= '{thirty_days_ago}'")
            
            where_clause = ' AND '.join(where_clauses) if where_clauses else None
            
            # Extract data
            all_records = []
            offset = 0
            limit = 1000
            max_cursor = None
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                while True:
                    try:
                        page = await self._fetch_page(client, offset, limit, where_clause)
                        
                        if not page:
                            break
                        
                        # Process records
                        for record in page:
                            processed = await self._process_record(record)
                            if processed:
                                all_records.append(processed)
                                
                                # Track max cursor for next run
                                if processed.get('status_date'):
                                    if not max_cursor or processed['status_date'] > max_cursor:
                                        max_cursor = processed['status_date']
                        
                        logger.info(f"Processed {len(page)} records (total: {len(all_records)})")
                        
                        if len(page) < limit:
                            break
                        
                        offset += limit
                        
                    except Exception as e:
                        logger.error(f"Error fetching page at offset {offset}: {e}")
                        if offset == 0:
                            raise
                        break
            
            self.records_extracted = len(all_records)
            
            if all_records:
                # Stage to Parquet
                staging_file = await self._stage_to_parquet(all_records)
                
                # Load to database
                await self._load_to_database(staging_file)
                
                # Log success
                self._log_audit(
                    status='success',
                    records_processed=self.records_extracted,
                    metadata={
                        'last_cursor': max_cursor,
                        'staging_file': str(staging_file)
                    }
                )
            else:
                logger.info("No new records to process")
                self._log_audit(
                    status='success',
                    records_processed=0,
                    metadata={'last_cursor': max_cursor}
                )
            
            return self.records_extracted
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            self._log_audit(
                status='failed',
                records_processed=self.records_extracted,
                error_message=str(e)
            )
            raise
    

    async def _process_record(self, raw_record: Dict) -> Dict:
        """Process and enrich a single permit record"""
        try:
            # Map fields
            processed = {}
            for socrata_field, our_field in self.FIELD_MAPPINGS.items():
                if socrata_field in raw_record:
                    processed[our_field] = raw_record[socrata_field]
            
            # Construct and normalize address
            raw_address = self._construct_address(raw_record)
            if raw_address:
                normalized = self.address_parser.parse(raw_address)
                processed['address_raw'] = raw_address
                processed['address_normalized'] = normalized.to_usps_format()
                processed['address_components'] = json.dumps(normalized.to_dict())
            
            # Add city, state, zip if not present
            if 'zip_code' in processed:
                processed['city'] = 'Los Angeles'
                processed['state'] = 'CA'
            
            # Extract units from description
            if processed.get('work_description'):
                units = self._extract_units_added(processed['work_description'])
                if units:
                    processed['units_added'] = units
            
            # Parse dates
            for date_field in ['issue_date', 'status_date']:
                if date_field in processed and processed[date_field]:
                    try:
                        # Handle Socrata date format
                        if 'T' in processed[date_field]:
                            processed[date_field] = processed[date_field].split('T')[0]
                    except:
                        pass
            
            # Convert valuation to float
            if 'valuation' in processed:
                try:
                    processed['valuation'] = float(processed['valuation'])
                except:
                    processed['valuation'] = None
            
            # Geocode if no coordinates
            if not processed.get('latitude') or not processed.get('longitude'):
                if processed.get('address_normalized'):
                    geo_result = await self.geocoder.geocode(
                        processed['address_normalized']
                    )
                    if geo_result and geo_result.status.name == 'SUCCESS':
                        processed['latitude'] = geo_result.latitude
                        processed['longitude'] = geo_result.longitude
                        processed['geocode_quality'] = geo_result.provider.name.lower()
                        processed['geocode_source'] = geo_result.provider.name
            
            # Add governance metadata
            processed['source_endpoint'] = f"{self.API_BASE}/{self.DATASET_ID}"
            processed['query_params'] = json.dumps({
                'extraction_id': self.extraction_id,
                'dataset': self.DATASET_ID
            })
            processed['as_of_date'] = datetime.now().date().isoformat()
            processed['ingest_timestamp'] = datetime.now().isoformat()
            
            # Generate natural key hash
            key_fields = ['permit_number', 'issue_date']
            key_values = [str(processed.get(f, '')) for f in key_fields]
            processed['natural_key_hash'] = hashlib.md5(
                '|'.join(key_values).encode()
            ).hexdigest()
            
            return processed
            
        except Exception as e:
            logger.warning(f"Error processing record {raw_record.get('permit_nbr')}: {e}")
            return None
    
    async def _stage_to_parquet(self, records: List[Dict]) -> Path:
        """Stage records to Parquet file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        staging_file = self.staging_dir / f"permits_{self.extraction_id}_{timestamp}.parquet"
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Convert data types to match schema
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce') 
        if 'valuation' in df.columns:
            df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce')
        if 'units_added' in df.columns:
            df['units_added'] = pd.to_numeric(df['units_added'], errors='coerce').astype('Int64')
        
        # Define schema for consistency
        schema = pa.schema([
            ('natural_key_hash', pa.string()),
            ('permit_number', pa.string()),
            ('permit_type', pa.string()),
            ('permit_subtype', pa.string()),
            ('status', pa.string()),
            ('status_date', pa.string()),
            ('issue_date', pa.string()),
            ('work_description', pa.string()),
            ('valuation', pa.float64()),
            ('units_added', pa.int64()),
            ('primary_address', pa.string()),
            ('address_raw', pa.string()),
            ('address_normalized', pa.string()),
            ('address_components', pa.string()),
            ('city', pa.string()),
            ('state', pa.string()),
            ('zip_code', pa.string()),
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('council_district', pa.string()),
            ('census_tract', pa.string()),
            ('zoning', pa.string()),
            ('area_planning_commission', pa.string()),
            ('community_plan_area', pa.string()),
            ('neighborhood_council', pa.string()),
            ('pin_number', pa.string()),
            ('assessor_parcel_number', pa.string()),
            ('use_code', pa.string()),
            ('use_description', pa.string()),
            ('permit_group', pa.string()),
            ('business_unit', pa.string()),
            ('electric_vehicle_ready', pa.string()),
            ('solar_ready', pa.string()),
            ('geocode_quality', pa.string()),
            ('source_endpoint', pa.string()),
            ('query_params', pa.string()),
            ('as_of_date', pa.string()),
            ('ingest_timestamp', pa.string()),
        ])
        
        # Write Parquet with schema
        table = pa.Table.from_pandas(df, schema=schema, safe=False)
        pq.write_table(table, staging_file, compression='snappy')
        
        logger.info(f"Staged {len(records)} records to {staging_file}")
        return staging_file
    
    async def _load_to_database(self, staging_file: Path):
        """Load staged Parquet file to database"""
        df = pd.read_parquet(staging_file)
        
        conn = self._get_connection()
        try:
            # Create temp table for staging
            conn.execute("""
                CREATE TEMP TABLE staging_permits AS
                SELECT * FROM raw_permits WHERE 1=0
            """)
            
            # Load data to staging
            df.to_sql('staging_permits', conn, if_exists='append', index=False)
            
            # Upsert to main table using natural key
            conn.execute("""
                INSERT OR REPLACE INTO raw_permits
                SELECT * FROM staging_permits
            """)
            
            conn.commit()
            logger.info(f"Loaded {len(df)} records to raw_permits")
            
        finally:
            conn.close()
    
    async def validate_extraction(self) -> Dict:
        """Validate extraction results"""
        conn = self._get_connection()
        try:
            # Check record counts
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT permit_number) as unique_permits,
                    COUNT(DISTINCT DATE(issue_date)) as days_covered,
                    MIN(issue_date) as earliest_date,
                    MAX(issue_date) as latest_date,
                    AVG(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as geocoding_rate,
                    AVG(CASE WHEN units_added IS NOT NULL THEN 1 ELSE 0 END) as units_extraction_rate
                FROM raw_permits
                WHERE DATE(ingest_timestamp) = DATE('now')
            """)
            
            stats = dict(cursor.fetchone())
            
            # Check for data quality issues
            issues = []
            
            if stats['geocoding_rate'] < 0.8:
                issues.append(f"Low geocoding rate: {stats['geocoding_rate']:.1%}")
            
            if stats['total_records'] == 0:
                issues.append("No records extracted")
            
            return {
                'status': 'warning' if issues else 'success',
                'stats': stats,
                'issues': issues
            }
            
        finally:
            conn.close()


async def main():
    """Main execution function"""
    extractor = LAPermitsExtractor()
    
    # Run extraction
    logger.info("Starting LA Building Permits extraction")
    records = await extractor.extract_permits(incremental=True)
    logger.info(f"Extracted {records} permit records")
    
    # Validate results
    validation = await extractor.validate_extraction()
    logger.info(f"Validation results: {validation}")
    
    return records


if __name__ == "__main__":
    asyncio.run(main())