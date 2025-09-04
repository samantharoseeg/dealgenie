#!/usr/bin/env bash
# Production Artifact Verification Script
# Implements CodeRabbit's suggested deterministic verification approach
# 
# Usage: ./scripts/verify_production_artifacts.sh
# Exit code: 0 if all artifacts present, 1 if any missing

set -euo pipefail

echo "🔍 DealGenie Production Artifact Verification"
echo "============================================="

files=(
  "verify_fixes.py"
  "db/database_manager.py"
  "ingest/census_acs.py"
  "ops/bootstrap.sh"
  "scripts/performance_benchmark_comprehensive.py"
  "config/scoring_config.json"
  "config/field_mappings/dealgenie_field_mapping.csv"
  "docs/data_provenance.md"
  "docs/database_architecture.md"
  "docs/feature_dictionary.md"
  "docs/FIELD_DOCUMENTATION.md"
  "db/sqlite_schema.sql"
  "db/ddl.sql"
  "VERIFICATION_GUIDE.md"
  ".coderabbit.yaml"
)

missing=0
found=0

for f in "${files[@]}"; do
  if [[ -e "$f" ]]; then
    echo "✅ FOUND  $f"
    ((found++))
  else
    echo "❌ MISSING $f" >&2
    ((missing++)) || true
  fi
done

echo "============================================="
echo "📊 Summary: $found found, $missing missing"

if [ $missing -eq 0 ]; then
    echo "🎉 All production artifacts verified successfully!"
    exit 0
else
    echo "⚠️  $missing artifact(s) missing - production system incomplete"
    exit 1
fi