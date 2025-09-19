# DealGenie - LA Property Intelligence Platform

## Project Overview
DealGenie is a comprehensive real estate analysis and visualization platform focused on Los Angeles properties. The system analyzes property data, scores development opportunities, and generates interactive maps for investment decision-making.

## Project Structure
```
dealgenie/
├── scraper/              # Data collection and processing scripts
│   └── create_la_property_map.py  # Interactive map visualization generator
├── data/                 # Property datasets and processed files
├── config/               # Configuration files
├── scripts/              # Utility and analysis scripts
├── sample_data/          # Sample datasets for testing
├── analysis/             # Analysis outputs
├── dashboard/            # Dashboard components
├── utils/                # Utility functions
└── docs/                 # Documentation
```

## Key Components

### 1. LA Property Map Visualization (`scraper/create_la_property_map.py`)
- **Purpose**: Generate interactive HTML maps showing enhanced scored properties
- **Features**:
  - Top 50 scoring properties color-coded by investment tier
  - Metro stations and transit lines overlay
  - Assembly opportunity clusters identification
  - Neighborhood boundaries and statistics
  - Interactive tooltips with property details
  - Multi-layer visualization with toggle controls

### 2. Data Files
- **Input Files**:
  - `geo_enhanced_scored_properties.csv` - Enhanced property data with scores
  - `geo_enhanced_scored_properties_assembly_opportunities.json` - Assembly opportunities
  - `geo_enhanced_scored_properties_neighborhood_analysis.json` - Neighborhood statistics
  
- **Geographic Data**:
  - `LA_City_Boundary.zip` - City boundary shapefiles
  - `LA_City_Parcels.zip` - Parcel geometry data
  - `LA_Zoning.zip` - Zoning information

### 3. Property Scoring System
- **Investment Tiers**:
  - A+ (Premium) - Red
  - A (Excellent) - Orange Red
  - B (Good) - Orange
  - C+ (Above Average) - Gold
  - C (Average) - Yellow
  - D (Below Average) - Light Blue

- **Geographic Bonuses**:
  - Location premium bonus
  - Transit accessibility bonus
  - Highway access bonus
  - Walkability score integration

## Development Guidelines

### Running the Map Generator
```bash
cd /Users/samanthagrant/Desktop/dealgenie/scraper
python create_la_property_map.py
```

### Required Dependencies
- pandas
- folium
- json
- logging

### Data Requirements
The map generator expects:
1. CSV file with enhanced property scores including:
   - property_id, site_address
   - latitude, longitude coordinates
   - enhanced_development_score
   - enhanced_investment_tier
   - geographic bonus fields
   - zoning and assessment data

2. JSON files with:
   - Assembly opportunities analysis
   - Neighborhood statistics

## Current Status
- Map visualization generator is complete
- Supports interactive exploration of top properties
- Includes transit overlay and assembly opportunities
- Generates HTML output for browser viewing

## TODO
- [ ] Add actual neighborhood boundary polygons
- [ ] Integrate real-time Metro API data
- [ ] Add heat map layer for property density
- [ ] Include school district overlays
- [ ] Add crime statistics layer
- [ ] Implement property comparison tool
- [ ] Add export functionality for selected properties

## Testing
Test the map generator with sample data:
```bash
python create_la_property_map.py
# Opens la_property_intelligence_map.html in browser
```

## Notes
- Map centers on Downtown LA (34.0522, -118.2437)
- Uses OpenStreetMap tiles with CartoDB alternatives
- Supports up to 50 properties for optimal performance
- Assembly opportunities limited to top 20 for clarity

## Development Workflow

### Git Branching Strategy
```
main
├── develop
│   ├── feature/map-enhancements
│   ├── feature/data-pipeline
│   └── feature/api-integration
├── hotfix/critical-bug
└── release/v1.1
```

**Branch Naming Conventions:**
- `feature/` - New features and enhancements
- `bugfix/` - Bug fixes that aren't critical
- `hotfix/` - Critical production fixes
- `release/` - Release preparation branches
- `docs/` - Documentation updates

### Pull Request Process
1. Create feature branch from `develop`
2. Make changes following code standards
3. Run tests locally: `pytest tests/`
4. Update documentation if needed
5. Create PR with description template:
   ```markdown
   ## Description
   Brief description of changes
   
   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update
   
   ## Testing
   - [ ] Unit tests pass
   - [ ] Integration tests pass
   - [ ] Manual testing completed
   
   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Self-review completed
   - [ ] Comments added for complex logic
   - [ ] Documentation updated
   ```

## Code Quality Standards

### Python Style Guide
- Follow PEP 8 with 100 character line limit
- Use Black for automatic formatting: `black . --line-length 100`
- Use isort for import sorting: `isort . --profile black`
- Use flake8 for linting: `flake8 . --max-line-length 100`

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
        language_version: python3.9
        args: [--line-length=100]
  
  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: [--profile=black]
  
  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=100, --extend-ignore=E203]
  
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.3.0
    hooks:
      - id: mypy
        additional_dependencies: [types-requests, types-pandas]
```

### Code Review Checklist
- [ ] No hardcoded credentials or API keys
- [ ] Proper error handling with specific exceptions
- [ ] Logging instead of print statements
- [ ] Type hints for all functions
- [ ] Docstrings for classes and public methods
- [ ] Unit tests for new functionality
- [ ] Performance considerations for large datasets

## Testing Requirements

### Test Structure
```
tests/
├── unit/
│   ├── test_property_scorer.py
│   ├── test_map_generator.py
│   └── test_data_processor.py
├── integration/
│   ├── test_pipeline.py
│   └── test_api_integration.py
├── fixtures/
│   ├── sample_properties.json
│   └── test_coordinates.csv
└── conftest.py
```

### Pytest Configuration
```ini
# pytest.ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --cov=scraper
    --cov=scripts
    --cov-report=term-missing
    --cov-report=html
    --cov-fail-under=80
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
    unit: marks tests as unit tests
```

### Writing Tests
```python
# Example test structure
import pytest
from typing import Dict, List
import pandas as pd
from scraper.create_la_property_map import LAPropertyMapGenerator

class TestPropertyMapGenerator:
    """Test suite for LA Property Map Generator"""
    
    @pytest.fixture
    def sample_properties(self) -> pd.DataFrame:
        """Fixture providing sample property data"""
        return pd.DataFrame({
            'property_id': [1, 2, 3],
            'latitude': [34.0522, 34.0623, 34.0724],
            'longitude': [-118.2437, -118.2538, -118.2639],
            'enhanced_development_score': [85.5, 72.3, 91.2],
            'enhanced_investment_tier': ['A', 'B', 'A+']
        })
    
    def test_map_initialization(self):
        """Test map generator initialization"""
        generator = LAPropertyMapGenerator()
        assert generator.la_center == [34.0522, -118.2437]
        assert 'A+' in generator.tier_colors
    
    def test_property_filtering(self, sample_properties):
        """Test filtering of valid properties with coordinates"""
        generator = LAPropertyMapGenerator()
        generator.properties_df = sample_properties
        # Add test assertions
```

### Test Coverage Requirements
- Minimum 80% code coverage
- 100% coverage for critical business logic
- All API endpoints must have integration tests
- Data validation functions require edge case testing

## CodeRabbit Integration

### Configuration
```yaml
# .coderabbit.yaml
language: python
reviews:
  auto_review:
    enabled: true
    ignore_patterns:
      - "*.md"
      - "tests/fixtures/*"
      - "*.html"
  
  request_changes_workflow: true
  high_level_summary: true
  poem: false
  review_status: true
  collapse_walkthrough: false
  
  path_instructions:
    - path: "scraper/**/*.py"
      instructions: "Focus on data validation and error handling"
    - path: "scripts/**/*.py"
      instructions: "Check for performance with large datasets"
    - path: "tests/**/*.py"
      instructions: "Ensure comprehensive test coverage"

tools:
  ruff:
    enabled: true
  mypy:
    enabled: true
  pylint:
    enabled: true
```

### CodeRabbit Commands
```bash
# In PR comments
@coderabbitai review       # Trigger review
@coderabbitai resolve      # Mark as resolved
@coderabbitai ignore       # Ignore file/pattern
@coderabbitai summary      # Generate PR summary
@coderabbitai help         # Show available commands
```

## Security Considerations

### Data Security
1. **PII Protection**
   - Never commit real property owner names
   - Anonymize sensitive address information in logs
   - Use environment variables for API keys
   
2. **API Security**
   ```python
   # Use environment variables
   import os
   from dotenv import load_dotenv
   
   load_dotenv()
   API_KEY = os.getenv('ZIMAS_API_KEY')
   DB_PASSWORD = os.getenv('DB_PASSWORD')
   ```

3. **Database Security**
   - Use parameterized queries to prevent SQL injection
   - Encrypt database connections
   - Rotate credentials regularly
   - Limit database user permissions

4. **File Handling**
   ```python
   # Secure file operations
   import os
   from pathlib import Path
   
   def safe_file_read(filepath: str) -> str:
       """Safely read file with validation"""
       path = Path(filepath).resolve()
       
       # Prevent directory traversal
       if not path.is_relative_to(Path.cwd()):
           raise ValueError("Invalid file path")
       
       if not path.exists():
           raise FileNotFoundError(f"File not found: {filepath}")
       
       return path.read_text()
   ```

### Dependency Security
```bash
# Regular security audits
pip-audit
safety check
bandit -r scraper/ scripts/

# Keep dependencies updated
pip list --outdated
pip-compile --upgrade requirements.in
```

## Python Coding Standards

### Type Hints
```python
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd

def process_properties(
    properties: pd.DataFrame,
    score_threshold: float = 70.0,
    max_results: Optional[int] = None
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Process and filter properties based on score threshold.
    
    Args:
        properties: DataFrame containing property data
        score_threshold: Minimum score for filtering
        max_results: Maximum number of results to return
    
    Returns:
        Tuple of filtered DataFrame and statistics dictionary
    
    Raises:
        ValueError: If score_threshold is negative
        KeyError: If required columns are missing
    """
    if score_threshold < 0:
        raise ValueError("Score threshold must be non-negative")
    
    # Implementation
    pass
```

### Docstring Format (Google Style)
```python
class PropertyAnalyzer:
    """Analyzes real estate properties for investment potential.
    
    This class provides methods for scoring, ranking, and visualizing
    property investment opportunities in Los Angeles.
    
    Attributes:
        scoring_weights (Dict[str, float]): Weights for scoring factors
        properties (pd.DataFrame): DataFrame of property data
        
    Example:
        >>> analyzer = PropertyAnalyzer()
        >>> analyzer.load_data("properties.csv")
        >>> scores = analyzer.calculate_scores()
    """
    
    def calculate_roi(
        self,
        purchase_price: float,
        rental_income: float,
        expenses: float
    ) -> float:
        """Calculate return on investment for a property.
        
        Args:
            purchase_price: Total purchase price including closing costs
            rental_income: Annual rental income
            expenses: Annual operating expenses
            
        Returns:
            ROI as a percentage
            
        Raises:
            ValueError: If purchase_price is zero or negative
            
        Note:
            This is a simplified ROI calculation. Consider using
            more sophisticated metrics for actual investment decisions.
        """
        pass
```

### Error Handling
```python
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class DataProcessingError(Exception):
    """Custom exception for data processing errors"""
    pass

def load_property_data(filepath: str) -> pd.DataFrame:
    """Load property data with comprehensive error handling."""
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} properties from {filepath}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"Empty CSV file: {filepath}")
        raise DataProcessingError(f"No data in {filepath}")
    except Exception as e:
        logger.exception(f"Unexpected error loading {filepath}")
        raise DataProcessingError(f"Failed to load data: {str(e)}")
```

### Logging Configuration
```python
# config/logging_config.py
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'logs/dealgenie.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
```

## Common Commands

### Project Setup
```bash
# Clone repository
git clone https://github.com/yourusername/dealgenie.git
cd dealgenie

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

### Development Commands
```bash
# Run the map generator
python scraper/create_la_property_map.py

# Run tests
pytest                          # Run all tests
pytest tests/unit               # Run unit tests only
pytest -m "not slow"           # Skip slow tests
pytest --cov                    # Run with coverage

# Code quality
black . --check                 # Check formatting
black .                         # Auto-format code
isort . --check-only           # Check import sorting
isort .                        # Auto-sort imports
flake8                         # Run linting
mypy scraper/                  # Type checking

# Security checks
bandit -r scraper/ scripts/    # Security linting
pip-audit                      # Check for vulnerable packages
safety check                   # Alternative security check
```

### Data Processing
```bash
# Process property data
python scripts/process_properties.py --input data/raw_properties.csv

# Generate enhanced scores
python scripts/enhance_property_scores.py

# Create assembly opportunities
python scripts/find_assembly_opportunities.py

# Generate map visualization
python scraper/create_la_property_map.py --top-n 100
```

### Database Operations
```bash
# Database migrations
alembic upgrade head           # Apply migrations
alembic revision -m "message"  # Create new migration

# Data export
python scripts/export_data.py --format csv --output exports/

# Data validation
python scripts/validate_data.py data/geo_enhanced_scored_properties.csv
```

### Deployment
```bash
# Build for production
python setup.py sdist bdist_wheel

# Run production server
gunicorn -w 4 -b 0.0.0.0:8000 app:application

# Docker operations
docker build -t dealgenie:latest .
docker run -p 8000:8000 dealgenie:latest
docker-compose up -d
```

### Troubleshooting
```bash
# Clear cache
find . -type d -name __pycache__ -exec rm -r {} +
find . -type f -name "*.pyc" -delete

# Reinstall dependencies
pip install --upgrade --force-reinstall -r requirements.txt

# Check environment
python -c "import sys; print(sys.version)"
pip list

# Debug mode
python -m pdb scraper/create_la_property_map.py
```

## Contact
Project: DealGenie AI Engine
Version: 1.0
Maintainer: Development Team
Support: support@dealgenie.ai