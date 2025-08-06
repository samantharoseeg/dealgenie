# DealGenie Workflow Guide 🔄

## Complete Data Processing Pipeline

This guide walks through the entire DealGenie workflow from raw ZIMAS data to scored investment opportunities.

## 📋 Prerequisites

- Python 3.7+
- Required packages: `pandas`, `numpy`, `json`
- ZIMAS database file (SQLite format)
- Minimum 2GB RAM for processing

## 🗂️ Workflow Steps

### Step 1: Database Analysis & Exploration
**Script**: `scripts/analyze_zimas_db.py`

**Purpose**: Understand the structure and quality of raw ZIMAS data

```bash
python scripts/analyze_zimas_db.py
```

**Outputs**:
- Database schema overview
- Record counts per table
- Field completeness analysis
- Sample data inspection

**Key Findings**:
- 78,747 properties with extracted ZIMAS data
- 193 unique fields per property
- 92.4% average completeness for critical fields

### Step 2: Data Cleaning & Standardization
**Script**: `scripts/clean_zimas_data.py`

**Purpose**: Transform messy ZIMAS data into analysis-ready format

```bash
python scripts/clean_zimas_data.py
```

**Cleaning Operations**:
1. **Parse JSON structures** - Extract nested property data
2. **Standardize APNs** - Format as XXXX-XXX-XXX
3. **Extract numeric values** - Remove text from lot sizes (e.g., "7,172.3 (sq ft)" → 7172.3)
4. **Clean currency fields** - Strip $ and commas from assessed values
5. **Normalize addresses** - Standardize street abbreviations and directions
6. **Extract base zoning** - Simplify complex codes (e.g., "R3-1VL-O" → "R3")
7. **Calculate derived fields** - FAR ratios, overlay counts, TOC eligibility

**Outputs**:
- `clean_zimas_ready_for_scoring.csv` - 1,000 sample properties
- `dealgenie_field_mapping.csv` - Field mapping documentation
- Data quality report with completeness metrics

### Step 3: Development Scoring
**Script**: `scripts/dealgenie_scorer.py`

**Purpose**: Calculate development potential scores using multi-factor algorithm

```bash
python scripts/dealgenie_scorer.py
```

**Scoring Components**:

#### Zoning Score (35% weight)
- **High Density**: R4, R5, RAS = 90-100 points
- **Medium Density**: R3, RD2.5 = 70-89 points
- **Commercial**: C2, C4, CM = 80-95 points
- **Industrial**: M1, M2 = 60-80 points
- **Low Density**: R1, R2 = 30-60 points

#### Lot Size Score (25% weight)
- **7,000+ sq ft**: 80-100 points (prime development size)
- **5,000-7,000 sq ft**: 60-80 points
- **3,000-5,000 sq ft**: 40-60 points
- **<3,000 sq ft**: 20-40 points

#### Transit Bonus (20% weight)
- **TOC Eligible**: +20 points (density bonus eligible)
- **High Quality Transit**: +15 points (Metro corridors)
- **Opportunity Corridors**: +10 points (OC-1, OC-2, etc.)

#### Financial Score (10% weight)
- **Land Value Ratio >70%**: +10 points (redevelopment ready)
- **FAR <0.5**: +5 points (underbuilt potential)
- **FAR <0.25**: +8 points (severely underbuilt)

#### Risk Penalties (10% weight)
- **Each Overlay Zone**: -3 points (regulatory complexity)
- **Methane Zones**: -5 points (environmental hazard)
- **Historic Districts**: -8 points (preservation constraints)
- **Fault Zones**: -4 points (seismic risk)

**Outputs**:
- `scored_zimas_properties.csv` - All properties with scores
- `scoring_config.json` - Configurable scoring parameters
- Summary analytics and top opportunities report

### Step 4: Results Analysis & Visualization
**Script**: `scripts/display_scored_properties.py`

**Purpose**: Generate formatted reports and investment insights

```bash
python scripts/display_scored_properties.py
```

**Analysis Reports**:
- Top 10 investment opportunities with detailed breakdowns
- Score distribution analysis
- TOC vs non-TOC performance comparison
- Council district opportunity rankings
- Use case recommendations by property type

## 📊 Key Output Files

### Primary Outputs
| File | Purpose | Records |
|------|---------|---------|
| `sample_data/zimas_property_sample_1000.csv` | Raw ZIMAS sample | 1,000 |
| `sample_data/clean_zimas_ready_for_scoring.csv` | Cleaned data | 1,000 |
| `sample_data/scored_zimas_properties.csv` | Final scored results | 1,000 |

### Configuration Files
| File | Purpose |
|------|---------|
| `config/scoring_config.json` | Scoring weights and parameters |
| `config/field_mappings/dealgenie_field_mapping.csv` | Field mapping documentation |

## 🔧 Customization Points

### 1. Modify Scoring Weights
Edit `config/scoring_config.json`:

```json
{
  "weights": {
    "zoning_score": 0.35,      // Adjust zoning importance
    "lot_size_score": 0.25,    // Emphasize lot size
    "transit_bonus": 0.20,     // TOC bonus weight
    "financial_score": 0.10,   // Financial indicators
    "risk_penalty": 0.10       // Risk constraints
  }
}
```

### 2. Add Custom Zoning Scores
Modify zoning scores in config:

```json
{
  "zoning_scores": {
    "R6": 105,          // New high-density zone
    "LAC": 85,          // Limited agricultural commercial
    "CUSTOM_ZONE": 75   // Custom scoring
  }
}
```

### 3. Adjust Lot Size Thresholds
Customize lot size scoring ranges:

```json
{
  "lot_size_thresholds": [
    {"min": 50000, "max": "inf", "score_range": [95, 100]},  // Mega lots
    {"min": 20000, "max": 50000, "score_range": [90, 95]},   // Large lots
    {"min": 10000, "max": 20000, "score_range": [80, 90]}    // Medium lots
  ]
}
```

## 🚀 Production Scaling

### Process Full Dataset (78,000+ properties)

```python
from scripts.clean_zimas_data import process_full_dataset

# Process complete database in batches
result_df = process_full_dataset(
    input_db_path='scraper/zimas_ajax_last_half.db',
    output_csv_path='all_scored_properties.csv',
    batch_size=10000
)

print(f"Processed {len(result_df)} total properties")
```

### Batch Processing Benefits
- **Memory Efficient**: Process 10k records at a time
- **Progress Tracking**: Real-time batch completion updates  
- **Error Recovery**: Continue from failed batches
- **Scalable**: Handle datasets of any size

## 📈 Performance Metrics

### Processing Speed
- **1,000 properties**: ~30 seconds
- **10,000 properties**: ~5 minutes
- **78,000 properties**: ~40 minutes (estimated)

### Data Quality Results
- **88%** average field completeness
- **98%** completeness for critical scoring fields
- **100%** APN and basic identifier coverage

### Memory Usage
- **Sample processing**: ~50MB RAM
- **Full dataset**: ~500MB RAM (with batching)
- **Output files**: ~20MB per 10k records

## 🔍 Quality Assurance

### Data Validation Checks
1. **APN Format Validation**: All APNs standardized to XXXX-XXX-XXX
2. **Numeric Range Checks**: Lot sizes, building sizes within reasonable bounds
3. **Score Range Validation**: All scores between 0-100
4. **Required Field Coverage**: Critical fields >90% complete

### Error Handling
- **JSON Parsing Errors**: Gracefully handled with fallbacks
- **Missing Data**: Scored using available fields with completeness penalty
- **Invalid Values**: Cleaned or excluded with logging

## 🎯 Investment Decision Framework

### Tier A Properties (80-100 points)
- **Action**: Immediate acquisition consideration
- **Focus**: Prime development opportunities
- **Timeline**: 0-6 months to market

### Tier B Properties (65-79 points)  
- **Action**: Detailed due diligence
- **Focus**: Strong development potential
- **Timeline**: 6-18 months development horizon

### Tier C Properties (50-64 points)
- **Action**: Monitor market conditions
- **Focus**: Moderate opportunity with specific use cases
- **Timeline**: 1-3 years strategic hold

### Tier D Properties (0-49 points)
- **Action**: Income/hold strategy
- **Focus**: Limited development upside
- **Timeline**: Long-term appreciation play

## 🔄 Workflow Optimization

### Speed Improvements
1. **Parallel Processing**: Run multiple scorer instances
2. **Database Indexing**: Add indexes on frequently queried fields
3. **Caching**: Cache cleaned data between scoring runs
4. **Vectorization**: Use pandas vectorized operations

### Accuracy Enhancements
1. **Market Data Integration**: Add recent sales, permit data
2. **Neighborhood Analysis**: Area-based scoring adjustments
3. **Seasonal Adjustments**: Account for market timing
4. **Regulatory Updates**: Keep zoning/overlay data current

## 🚨 Common Issues & Solutions

### Issue: "FileNotFoundError: scoring_config.json"
**Solution**: Ensure config files are in correct directory structure
```bash
ls config/scoring_config.json  # Should exist
```

### Issue: Low scores across all properties
**Solution**: Check scoring weights and zoning classifications
```python
# Review config settings
with open('config/scoring_config.json') as f:
    config = json.load(f)
    print(config['weights'])
```

### Issue: Memory errors on large datasets
**Solution**: Reduce batch size in processing
```python
# Reduce from 10000 to 5000
process_full_dataset(..., batch_size=5000)
```

---

*This workflow produces actionable real estate intelligence for LA development opportunities* 🏗️