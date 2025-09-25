# Data Reliability Developer Guide

## Overview
This guide provides developers with practical implementation patterns for working with environmental data sources in the DealGenie platform. It covers source hierarchy implementation, confidence scoring, conflict resolution, and performance optimization for our 457,768 property dataset.

## Quick Reference

### Source Authority Hierarchy
```python
# Authority ranking for flood zone data (highest to lowest)
SOURCE_HIERARCHY = {
    1: {"name": "FEMA NFHL", "confidence": 0.98, "type": "authoritative"},
    2: {"name": "FEMA MSC", "confidence": 0.98, "type": "authoritative"},
    3: {"name": "ZIMAS", "confidence": 0.85, "type": "municipal"},
    4: {"name": "USGS", "confidence": 0.80, "type": "scientific"},
    5: {"name": "Commercial", "confidence": 0.70, "type": "commercial"}
}
```

### Essential Imports
```python
import psycopg2
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
```

## Core Implementation Patterns

### 1. Source-Aware Data Retrieval

#### Basic Pattern
```python
def get_flood_zone_with_confidence(apn: str) -> Dict[str, any]:
    """
    Retrieve flood zone using source hierarchy with confidence scoring.

    Args:
        apn: Property APN identifier

    Returns:
        Dict containing flood_zone, source_used, confidence_score, and metadata
    """
    query = """
    SELECT
        COALESCE(
            fema_nfhl.flood_zone,
            fema_msc.flood_zone,
            zimas.zimas_flood_zone,
            'Unknown'
        ) as flood_zone,
        CASE
            WHEN fema_nfhl.flood_zone IS NOT NULL THEN 'FEMA_NFHL'
            WHEN fema_msc.flood_zone IS NOT NULL THEN 'FEMA_MSC'
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 'ZIMAS'
            ELSE 'NONE'
        END as source_used,
        CASE
            WHEN fema_nfhl.flood_zone IS NOT NULL THEN 0.98
            WHEN fema_msc.flood_zone IS NOT NULL THEN 0.98
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 0.85
            ELSE 0.0
        END as confidence_score,
        CASE
            WHEN fema_nfhl.flood_zone IS NOT NULL THEN fema_nfhl.verification_date
            WHEN fema_msc.flood_zone IS NOT NULL THEN fema_msc.verification_date
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN zimas.import_date
            ELSE NULL
        END as last_verified
    FROM unified_property_data p
    LEFT JOIN fema_flood_zones fema_nfhl ON p.apn = fema_nfhl.apn
        AND fema_nfhl.verification_method = 'NFHL_API'
    LEFT JOIN fema_flood_zones fema_msc ON p.apn = fema_msc.apn
        AND fema_msc.verification_method = 'manual_verification'
    LEFT JOIN zimas_flood_zones zimas ON p.apn = zimas.apn
    WHERE p.apn = %s;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (apn,))
            result = cursor.fetchone()

            if result:
                return {
                    'apn': apn,
                    'flood_zone': result[0],
                    'source_used': result[1],
                    'confidence_score': float(result[2]),
                    'last_verified': result[3],
                    'requires_verification': result[2] < 0.90
                }
            else:
                return {
                    'apn': apn,
                    'flood_zone': 'Unknown',
                    'source_used': 'NONE',
                    'confidence_score': 0.0,
                    'last_verified': None,
                    'requires_verification': True
                }
```

#### Batch Pattern for Performance
```python
def get_flood_zones_batch(apns: List[str]) -> Dict[str, Dict]:
    """
    Efficiently retrieve flood zones for multiple properties.

    Args:
        apns: List of property APN identifiers

    Returns:
        Dictionary mapping APN to flood zone data
    """
    if not apns:
        return {}

    # Use ANY() for efficient batch querying
    query = """
    SELECT
        p.apn,
        COALESCE(
            fema_nfhl.flood_zone,
            fema_msc.flood_zone,
            zimas.zimas_flood_zone,
            'Unknown'
        ) as flood_zone,
        CASE
            WHEN fema_nfhl.flood_zone IS NOT NULL THEN 'FEMA_NFHL'
            WHEN fema_msc.flood_zone IS NOT NULL THEN 'FEMA_MSC'
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 'ZIMAS'
            ELSE 'NONE'
        END as source_used,
        CASE
            WHEN fema_nfhl.flood_zone IS NOT NULL THEN 0.98
            WHEN fema_msc.flood_zone IS NOT NULL THEN 0.98
            WHEN zimas.zimas_flood_zone IS NOT NULL THEN 0.85
            ELSE 0.0
        END as confidence_score
    FROM unified_property_data p
    LEFT JOIN fema_flood_zones fema_nfhl ON p.apn = fema_nfhl.apn
        AND fema_nfhl.verification_method = 'NFHL_API'
    LEFT JOIN fema_flood_zones fema_msc ON p.apn = fema_msc.apn
        AND fema_msc.verification_method = 'manual_verification'
    LEFT JOIN zimas_flood_zones zimas ON p.apn = zimas.apn
    WHERE p.apn = ANY(%s);
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (apns,))
            results = cursor.fetchall()

            return {
                row[0]: {
                    'flood_zone': row[1],
                    'source_used': row[2],
                    'confidence_score': float(row[3])
                }
                for row in results
            }
```

### 2. Conflict Detection and Resolution

#### Detect Source Conflicts
```python
def detect_flood_zone_conflicts(limit: int = 100) -> List[Dict]:
    """
    Identify properties where different sources provide conflicting flood zone data.

    Args:
        limit: Maximum number of conflicts to return

    Returns:
        List of conflict records with source details
    """
    query = """
    SELECT
        p.apn,
        p.site_address,
        p.latitude,
        p.longitude,
        fema.flood_zone as fema_zone,
        fema.verification_date as fema_verified,
        zimas.zimas_flood_zone as zimas_zone,
        zimas.import_date as zimas_verified,
        ABS(EXTRACT(EPOCH FROM (fema.verification_date - zimas.import_date))/86400) as days_between_updates
    FROM unified_property_data p
    INNER JOIN fema_flood_zones fema ON p.apn = fema.apn
    INNER JOIN zimas_flood_zones zimas ON p.apn = zimas.apn
    WHERE fema.flood_zone != zimas.zimas_flood_zone
    ORDER BY days_between_updates DESC
    LIMIT %s;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (limit,))
            results = cursor.fetchall()

            conflicts = []
            for row in results:
                conflict = {
                    'apn': row[0],
                    'address': row[1],
                    'coordinates': {'lat': float(row[2]), 'lng': float(row[3])},
                    'fema_zone': row[4],
                    'fema_verified': row[5],
                    'zimas_zone': row[6],
                    'zimas_verified': row[7],
                    'days_between_updates': int(row[8]) if row[8] else 0,
                    'resolution': 'Use FEMA as authoritative source',
                    'verification_url': f"https://msc.fema.gov/portal/search#{row[2]},{row[3]}"
                }
                conflicts.append(conflict)

            return conflicts
```

#### Automatic Conflict Resolution
```python
def resolve_flood_zone_conflict(apn: str, create_flag: bool = True) -> Dict:
    """
    Resolve flood zone conflict by applying source hierarchy rules.

    Args:
        apn: Property APN identifier
        create_flag: Whether to create data quality flag for manual review

    Returns:
        Resolution details with authoritative determination
    """
    # Get all available sources for this property
    sources = get_all_flood_zone_sources(apn)

    if len(sources) <= 1:
        return {'status': 'no_conflict', 'sources': sources}

    # Check for conflicts
    unique_zones = set(source['flood_zone'] for source in sources if source['flood_zone'] != 'Unknown')

    if len(unique_zones) <= 1:
        return {'status': 'no_conflict', 'sources': sources}

    # Resolve using hierarchy (FEMA > ZIMAS > others)
    authoritative_source = min(sources, key=lambda x: x['authority_rank'])

    resolution = {
        'status': 'conflict_resolved',
        'apn': apn,
        'authoritative_zone': authoritative_source['flood_zone'],
        'authoritative_source': authoritative_source['source_name'],
        'confidence_score': authoritative_source['confidence_score'],
        'conflicting_sources': [s for s in sources if s['flood_zone'] != authoritative_source['flood_zone']],
        'resolution_date': datetime.now().isoformat()
    }

    # Create data quality flag if requested
    if create_flag:
        create_conflict_flag(apn, resolution)

    return resolution

def create_conflict_flag(apn: str, resolution: Dict):
    """Create data quality flag for flood zone conflict."""
    flag_data = {
        'discrepancy_type': 'flood_zone_conflict',
        'authoritative_value': resolution['authoritative_zone'],
        'authoritative_source': resolution['authoritative_source'],
        'conflicting_sources': [
            {
                'source': s['source_name'],
                'value': s['flood_zone'],
                'confidence': s['confidence_score']
            }
            for s in resolution['conflicting_sources']
        ],
        'resolution': 'Use highest authority source',
        'requires_verification': resolution['confidence_score'] < 0.95,
        'flagged_date': resolution['resolution_date']
    }

    # Insert into data quality flags
    update_query = """
    UPDATE zimas_flood_zones
    SET data_quality_flags = %s::jsonb
    WHERE apn = %s;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update_query, (json.dumps(flag_data), apn))
            conn.commit()
```

### 3. Data Freshness and Validation

#### Check Data Freshness
```python
def check_data_freshness(days_threshold: int = 90) -> Dict[str, List]:
    """
    Identify environmental data that may be stale and require updates.

    Args:
        days_threshold: Number of days after which data is considered stale

    Returns:
        Dictionary of stale data by source type
    """
    cutoff_date = datetime.now() - timedelta(days=days_threshold)

    query = """
    SELECT
        dsh.source_name,
        COUNT(*) as record_count,
        MIN(CASE
            WHEN dsh.source_name = 'FEMA NFHL' THEN f.verification_date
            WHEN dsh.source_name = 'ZIMAS' THEN z.import_date
        END) as oldest_record,
        MAX(CASE
            WHEN dsh.source_name = 'FEMA NFHL' THEN f.verification_date
            WHEN dsh.source_name = 'ZIMAS' THEN z.import_date
        END) as newest_record
    FROM data_source_hierarchy dsh
    LEFT JOIN fema_flood_zones f ON dsh.source_name = 'FEMA NFHL'
    LEFT JOIN zimas_flood_zones z ON dsh.source_name = 'ZIMAS'
    WHERE dsh.data_category = 'flood_zones'
    AND (
        (dsh.source_name = 'FEMA NFHL' AND f.verification_date < %s) OR
        (dsh.source_name = 'ZIMAS' AND z.import_date < %s)
    )
    GROUP BY dsh.source_name
    ORDER BY oldest_record;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (cutoff_date, cutoff_date))
            results = cursor.fetchall()

            stale_data = {}
            for row in results:
                stale_data[row[0]] = {
                    'record_count': row[1],
                    'oldest_record': row[2],
                    'newest_record': row[3],
                    'requires_update': True
                }

            return stale_data
```

#### Validate Data Integrity
```python
def validate_environmental_data() -> Dict[str, any]:
    """
    Comprehensive validation of environmental data integrity.

    Returns:
        Validation report with issues and recommendations
    """
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'total_properties': 0,
        'coverage_stats': {},
        'data_quality_issues': [],
        'recommendations': []
    }

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Total property count
            cursor.execute("SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL;")
            validation_results['total_properties'] = cursor.fetchone()[0]

            # Coverage statistics
            coverage_query = """
            SELECT
                'FEMA' as source_type,
                COUNT(*) as coverage_count,
                ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL)) * 100, 2) as coverage_pct
            FROM unified_property_data p
            INNER JOIN fema_flood_zones f ON p.apn = f.apn
            WHERE p.geom IS NOT NULL

            UNION ALL

            SELECT
                'ZIMAS' as source_type,
                COUNT(*) as coverage_count,
                ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM unified_property_data WHERE geom IS NOT NULL)) * 100, 2) as coverage_pct
            FROM unified_property_data p
            INNER JOIN zimas_flood_zones z ON p.apn = z.apn
            WHERE p.geom IS NOT NULL;
            """

            cursor.execute(coverage_query)
            coverage_results = cursor.fetchall()

            for row in coverage_results:
                validation_results['coverage_stats'][row[0]] = {
                    'count': row[1],
                    'percentage': float(row[2])
                }

            # Data quality issues
            quality_query = """
            SELECT
                'Missing FEMA verification' as issue_type,
                COUNT(*) as issue_count
            FROM unified_property_data p
            LEFT JOIN fema_flood_zones f ON p.apn = f.apn
            WHERE p.geom IS NOT NULL AND f.apn IS NULL

            UNION ALL

            SELECT
                'Source conflicts' as issue_type,
                COUNT(*) as issue_count
            FROM unified_property_data p
            INNER JOIN fema_flood_zones f ON p.apn = f.apn
            INNER JOIN zimas_flood_zones z ON p.apn = z.apn
            WHERE f.flood_zone != z.zimas_flood_zone

            UNION ALL

            SELECT
                'Stale data (>90 days)' as issue_type,
                COUNT(*) as issue_count
            FROM zimas_flood_zones z
            WHERE z.import_date < NOW() - INTERVAL '90 days';
            """

            cursor.execute(quality_query)
            quality_results = cursor.fetchall()

            for row in quality_results:
                if row[1] > 0:  # Only include issues with count > 0
                    validation_results['data_quality_issues'].append({
                        'issue_type': row[0],
                        'count': row[1]
                    })

    # Generate recommendations based on findings
    if validation_results['coverage_stats'].get('FEMA', {}).get('percentage', 0) < 50:
        validation_results['recommendations'].append(
            "Low FEMA coverage detected. Consider bulk FEMA data import or manual verification campaign."
        )

    if any(issue['issue_type'] == 'Source conflicts' for issue in validation_results['data_quality_issues']):
        validation_results['recommendations'].append(
            "Source conflicts detected. Run conflict resolution process to update authoritative values."
        )

    if any(issue['issue_type'] == 'Stale data (>90 days)' for issue in validation_results['data_quality_issues']):
        validation_results['recommendations'].append(
            "Stale data detected. Schedule data refresh from source systems."
        )

    return validation_results
```

### 4. Performance Optimization

#### Caching Strategy
```python
import redis
import json
from functools import wraps

# Redis client for caching
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def cache_flood_zone(ttl_seconds: int = 3600):
    """Decorator to cache flood zone lookups."""
    def decorator(func):
        @wraps(func)
        def wrapper(apn: str, *args, **kwargs):
            cache_key = f"flood_zone:{apn}"

            # Try to get from cache
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return json.loads(cached_result)

            # Get from database
            result = func(apn, *args, **kwargs)

            # Cache the result
            redis_client.setex(cache_key, ttl_seconds, json.dumps(result, default=str))

            return result
        return wrapper
    return decorator

@cache_flood_zone(ttl_seconds=3600)  # Cache for 1 hour
def get_cached_flood_zone(apn: str) -> Dict:
    """Cached version of flood zone lookup."""
    return get_flood_zone_with_confidence(apn)
```

#### Bulk Processing
```python
def process_flood_zones_bulk(apns: List[str], batch_size: int = 1000) -> Dict[str, Dict]:
    """
    Process flood zones in batches for optimal performance.

    Args:
        apns: List of APNs to process
        batch_size: Number of APNs to process in each batch

    Returns:
        Dictionary mapping APN to flood zone data
    """
    results = {}

    # Process in batches
    for i in range(0, len(apns), batch_size):
        batch = apns[i:i + batch_size]
        logging.info(f"Processing batch {i//batch_size + 1}/{(len(apns)//batch_size) + 1}")

        batch_results = get_flood_zones_batch(batch)
        results.update(batch_results)

        # Small delay to prevent database overload
        time.sleep(0.1)

    return results
```

### 5. Error Handling and Logging

#### Robust Error Handling
```python
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class DataReliabilityError(Exception):
    """Custom exception for data reliability issues."""
    pass

def safe_get_flood_zone(apn: str) -> Optional[Dict]:
    """
    Safely retrieve flood zone with comprehensive error handling.

    Args:
        apn: Property APN identifier

    Returns:
        Flood zone data or None if error occurs
    """
    try:
        result = get_flood_zone_with_confidence(apn)

        # Validate result structure
        required_fields = ['flood_zone', 'source_used', 'confidence_score']
        if not all(field in result for field in required_fields):
            raise DataReliabilityError(f"Incomplete flood zone data for APN {apn}")

        # Log low confidence scores
        if result['confidence_score'] < 0.80:
            logger.warning(f"Low confidence flood zone data for APN {apn}: {result['confidence_score']}")

        return result

    except psycopg2.Error as e:
        logger.error(f"Database error getting flood zone for APN {apn}: {e}")
        return None
    except DataReliabilityError as e:
        logger.error(f"Data reliability error for APN {apn}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error getting flood zone for APN {apn}: {e}")
        return None
```

### 6. Monitoring and Alerting

#### Data Quality Metrics
```python
def calculate_data_quality_metrics() -> Dict[str, float]:
    """
    Calculate key data quality metrics for monitoring.

    Returns:
        Dictionary of quality metrics
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            metrics = {}

            # Overall coverage percentage
            cursor.execute("""
                SELECT
                    (COUNT(CASE WHEN f.apn IS NOT NULL OR z.apn IS NOT NULL THEN 1 END)::float /
                     COUNT(*)::float) * 100 as overall_coverage
                FROM unified_property_data p
                LEFT JOIN fema_flood_zones f ON p.apn = f.apn
                LEFT JOIN zimas_flood_zones z ON p.apn = z.apn
                WHERE p.geom IS NOT NULL;
            """)
            metrics['overall_coverage_pct'] = cursor.fetchone()[0]

            # Average confidence score
            cursor.execute("""
                SELECT AVG(
                    CASE
                        WHEN f.apn IS NOT NULL THEN 0.98
                        WHEN z.apn IS NOT NULL THEN 0.85
                        ELSE 0.0
                    END
                ) as avg_confidence
                FROM unified_property_data p
                LEFT JOIN fema_flood_zones f ON p.apn = f.apn
                LEFT JOIN zimas_flood_zones z ON p.apn = z.apn
                WHERE p.geom IS NOT NULL;
            """)
            metrics['avg_confidence_score'] = float(cursor.fetchone()[0])

            # Conflict rate
            cursor.execute("""
                SELECT
                    (COUNT(CASE WHEN f.flood_zone != z.zimas_flood_zone THEN 1 END)::float /
                     COUNT(*)::float) * 100 as conflict_rate
                FROM fema_flood_zones f
                INNER JOIN zimas_flood_zones z ON f.apn = z.apn;
            """)
            result = cursor.fetchone()
            metrics['conflict_rate_pct'] = float(result[0]) if result[0] else 0.0

            return metrics

def check_data_quality_thresholds(metrics: Dict[str, float]) -> List[str]:
    """
    Check if data quality metrics meet acceptable thresholds.

    Args:
        metrics: Dictionary of calculated metrics

    Returns:
        List of alerts for metrics that don't meet thresholds
    """
    alerts = []

    # Define thresholds
    thresholds = {
        'overall_coverage_pct': 95.0,      # Minimum 95% coverage
        'avg_confidence_score': 0.85,      # Minimum 85% average confidence
        'conflict_rate_pct': 5.0           # Maximum 5% conflict rate
    }

    for metric, threshold in thresholds.items():
        if metric in metrics:
            value = metrics[metric]

            if metric == 'conflict_rate_pct':
                # Alert if conflict rate is too high
                if value > threshold:
                    alerts.append(f"High conflict rate: {value:.2f}% (threshold: {threshold}%)")
            else:
                # Alert if coverage or confidence is too low
                if value < threshold:
                    alerts.append(f"Low {metric}: {value:.2f} (threshold: {threshold})")

    return alerts
```

## Usage Examples

### Basic Property Lookup
```python
# Simple flood zone lookup
flood_data = get_flood_zone_with_confidence("5036019013")
print(f"Flood Zone: {flood_data['flood_zone']}")
print(f"Source: {flood_data['source_used']}")
print(f"Confidence: {flood_data['confidence_score']}")
```

### Batch Processing
```python
# Process multiple properties efficiently
apns = ["5036019013", "2163008017", "5116016020"]
results = get_flood_zones_batch(apns)

for apn, data in results.items():
    print(f"APN {apn}: {data['flood_zone']} ({data['source_used']})")
```

### Conflict Detection
```python
# Find and resolve conflicts
conflicts = detect_flood_zone_conflicts(limit=10)
for conflict in conflicts:
    print(f"Conflict at {conflict['apn']}: FEMA={conflict['fema_zone']}, ZIMAS={conflict['zimas_zone']}")
    resolution = resolve_flood_zone_conflict(conflict['apn'])
    print(f"Resolution: Use {resolution['authoritative_source']} - {resolution['authoritative_zone']}")
```

### Data Quality Monitoring
```python
# Monitor data quality
metrics = calculate_data_quality_metrics()
alerts = check_data_quality_thresholds(metrics)

if alerts:
    for alert in alerts:
        logger.warning(f"Data quality alert: {alert}")
else:
    logger.info("All data quality metrics within acceptable thresholds")
```

## Best Practices Summary

1. **Always use source hierarchy**: Query sources in authority rank order
2. **Implement caching**: Cache frequently accessed data to improve performance
3. **Handle conflicts gracefully**: Use automated resolution with manual review flags
4. **Monitor data freshness**: Regular validation and refresh cycles
5. **Log data lineage**: Track which source was used for each determination
6. **Validate inputs**: Check APN format and existence before queries
7. **Use batch processing**: Process multiple properties efficiently
8. **Implement circuit breakers**: Fail gracefully when external sources are unavailable
9. **Monitor quality metrics**: Set up alerting for data quality degradation
10. **Document decisions**: Log reasoning for conflict resolutions and source selections

---

**Last Updated**: 2025-01-23
**Version**: 1.0
**Maintainer**: DealGenie Data Engineering Team