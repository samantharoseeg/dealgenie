#!/usr/bin/env python3
"""
DealGenie Setup Configuration
AI-Powered Real Estate Development Intelligence Platform
"""

from setuptools import setup, find_packages
import os

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements from requirements.txt
def read_requirements():
    requirements = []
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and not line.startswith("-"):
                    # Extract package name (remove version specifiers and comments)
                    package = line.split(">=")[0].split("==")[0].split("#")[0].strip()
                    if package:
                        requirements.append(line)
    return requirements

setup(
    name="dealgenie",
    version="2.0.0",
    author="DealGenie Team",
    author_email="team@dealgenie.ai",
    description="AI-Powered Real Estate Development Intelligence Platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/dealgenie",
    project_urls={
        "Bug Tracker": "https://github.com/your-org/dealgenie/issues",
        "Documentation": "https://docs.dealgenie.ai",
        "Source Code": "https://github.com/your-org/dealgenie",
    },
    
    # Package configuration
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    
    # Include data files
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yml", "*.yaml"],
    },
    
    # Dependencies
    install_requires=read_requirements(),
    
    # Optional dependencies
    extras_require={
        "libpostal": ["postal>=1.1.0"],
        "performance": ["python-Levenshtein>=0.12.0"],
        "postgres": ["psycopg2>=2.9.0"],
        "ml": ["scikit-learn>=1.1.0", "pandas>=1.5.0", "numpy>=1.21.0"],
        "visualization": ["matplotlib>=3.5.0", "seaborn>=0.11.0"],
        "api": ["fastapi>=0.85.0", "uvicorn>=0.18.0"],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "memory_profiler>=0.60.0",
            "line_profiler>=4.0.0",
        ],
    },
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Classification
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    
    # Entry points for command-line tools
    entry_points={
        "console_scripts": [
            "dealgenie-score=cli.dg_score:main",
            "dealgenie-health=scripts.daily_health_check:main",
        ],
    },
    
    # Keywords for PyPI
    keywords=[
        "real estate", "development", "analysis", "geocoding", 
        "address parsing", "los angeles", "property scoring",
        "investment analysis", "gis", "spatial analysis"
    ],
)