"""
Settings: Application configuration using pydantic-settings

Provides configuration management with:
- Environment variable support (.env file)
- Type validation
- Feature flags for Monte Carlo
- Database connection settings

Environment variables can be set in .env file or shell.
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Supports .env file via pydantic-settings.
    All values have sensible defaults for development.
    """

    # Database connection
    DATABASE_URL: str = "postgresql://samanthagrant@localhost/dealgenie_production"

    # Monte Carlo feature flags
    MONTE_CARLO_ENABLED: bool = False  # Feature flag: enable actual MC simulation
    CAPTURE_SIM_INPUTS: bool = True    # Always capture inputs for reproducibility

    # Monte Carlo defaults
    MC_DEFAULT_RUNS: int = 1000        # Number of scenarios to run
    MC_DEFAULT_SEED: int = 42          # Random seed for reproducibility
    MC_SIM_VERSION: int = 1            # Simulation methodology version

    # Application settings
    APP_NAME: str = "DealGenie"
    DEBUG: bool = False

    class Config:
        """Pydantic config for settings."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """
    Get application settings instance.

    Returns:
        Settings instance with current configuration

    Example:
        >>> from config.settings import get_settings
        >>> settings = get_settings()
        >>> print(settings.MONTE_CARLO_ENABLED)
        False
    """
    return settings
