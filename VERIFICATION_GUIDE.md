# DealGenie Verification Guide

**Quick Manual Verification Commands**

This guide provides simple commands to verify that all DealGenie fixes are working correctly.

## 🚀 Complete System Test (30 seconds)

```bash
# Run full bootstrap pipeline - tests everything
make bootstrap

# Expected: Completes in under 10 seconds, generates 15+ HTML reports
# Look for: "Bootstrap pipeline completed successfully" message
```

## 🏠 Verify HTML Reports Show Real Addresses

```bash
# Check that reports contain actual property addresses (not placeholders)
grep -l "OAKMORE ROAD\|LOUISE AVE\|HERCULES DR" out/dealgenie_report_*.html

# Expected: Multiple HTML files listed
# Manual check: Open any report - should show real LA County addresses
open out/dealgenie_report_4306026007_multifamily.html
```

## 🌍 Verify Census API Integration

```bash
# Test Census API with known working APN
python3 ingest/census_acs.py single --apn 4306026007

# Expected: JSON output with demographic data like:
# "median_income": 68972, "population": 3542, etc.
# No JSON decode errors
```

## 💾 Verify Database Operations

```bash
# Check SQLite database is populated
python3 db/database_manager.py info

# Expected output includes:
# - "32 zoning codes loaded"
# - "Parcels table: X records"
# - "Database status: OPERATIONAL"
```

## 🧪 Verify Feature Extraction

```bash
# Test feature extraction for single property
python3 cli/dg_score.py score --template multifamily --apn 4306026007

# Expected: JSON with 44+ features including:
# - "site_address": real address
# - "zoning": actual zoning code  
# - "lot_size": numeric value
# - Overall score with component breakdown
```

## ⚡ Verify Performance

```bash
# Quick performance test
python3 scripts/performance_benchmark_simple.py

# Expected: 
# - Processing time under 1 second per property
# - "Performance validation: PASSED"
# - No timeout errors
```

## 📋 Quick Checklist

Run these commands in order and verify each passes:

- [ ] `make bootstrap` - Completes successfully under 10 seconds
- [ ] `ls out/` - Shows 15+ HTML report files  
- [ ] `open out/dealgenie_report_*.html` - Reports show real addresses
- [ ] `python3 ingest/census_acs.py single --apn 4306026007` - Returns demographic data
- [ ] `python3 db/database_manager.py info` - Shows operational database
- [ ] `python3 cli/dg_score.py score --template multifamily --apn 4306026007` - Returns complete scoring

## 🚨 Troubleshooting

**Bootstrap fails**: Check that `scraper/la_parcels_complete_merged.csv` exists (581MB file)

**No HTML reports**: Verify `out/` directory exists and is writable

**Census API errors**: Check internet connection, API may have rate limits

**Database issues**: Run `python3 db/database_manager.py init` to reinitialize

**Missing features**: Verify CSV file has all 210 columns with real LA County data

## ✅ Success Indicators

System is working correctly when:
- Bootstrap completes in under 10 seconds
- HTML reports display actual LA County addresses like "9406 W OAKMORE ROAD"
- Census API returns real demographic data without JSON errors
- Database shows 32+ zoning codes and operational status
- Feature extraction produces 44+ features with real property data
- Performance tests show processing speeds of 10+ parcels/second

---

**For automated verification, run:** `python3 verify_fixes.py`