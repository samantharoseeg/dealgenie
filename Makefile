# DealGenie Week 1 Foundation Makefile
# Complete pipeline automation

.PHONY: all bootstrap clean test reports setup-dirs foundation

# Default target - runs complete Week 1 foundation pipeline
all: bootstrap

# Week 1 Foundation Bootstrap - Complete pipeline
bootstrap: setup-dirs
	@./ops/bootstrap_simplified.sh

# Foundation alias for bootstrap
foundation: bootstrap

# Create required directories
setup-dirs:
	@echo "📁 Setting up directories..."
	@mkdir -p out/
	@mkdir -p data/
	@mkdir -p logs/

# Quick test - single APN scoring
test:
	@echo "🧪 Testing DealGenie scoring..."
	@python3 cli/dg_score.py score --template multifamily --apn 4306026007

# Generate additional reports
reports:
	@echo "📊 Generating additional reports..."
	@python3 -c "\
import os, sys; \
sys.path.append('.'); \
from features.feature_matrix import get_feature_matrix; \
from scoring.engine import calculate_score; \
apns = ['4306026008', '4306026009', '2031007060']; \
templates = ['residential', 'commercial', 'multifamily']; \
[print(f'APN {apn} ({templates[i % len(templates)]}): {calculate_score(get_feature_matrix(apn), templates[i % len(templates)])[\"score\"]:.1f}/10') for i, apn in enumerate(apns)]"

# Clean generated files
clean:
	@echo "🧹 Cleaning generated files..."
	@rm -rf out/*.html
	@rm -rf data/*.db
	@rm -rf logs/*.log
	@echo "✓ Cleaned output files"

# Help
help:
	@echo "DealGenie Week 1 Foundation Commands:"
	@echo "  make bootstrap  - Run complete Week 1 pipeline"
	@echo "  make test      - Test single APN scoring"
	@echo "  make reports   - Generate additional reports"
	@echo "  make clean     - Clean generated files"
	@echo ""
	@echo "Week 1 delivers: Real LA County data integration + HTML reports"