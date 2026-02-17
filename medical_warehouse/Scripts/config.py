import os
from pydantic_settings import BaseSettings
from dataclasses import dataclass

@dataclass(frozen=True)
class ProjectConstants:
    """Fixed constants for the ELT pipeline."""
    RAW_SCHEMA: str = "raw"
    PROCESSED_SCHEMA: str = "processed"
    MSG_TABLE: str = "telegram_messages"
    ANALYSIS_TABLE: str = "image_analysis"
    
    # Path logic: Go up two levels from /medical_warehouse/Scripts to reach project root
    BASE_DATA_DIR: str = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../data")
    )
    IMAGE_SUBDIR: str = "raw/images"
    JSON_SUBDIR: str = "raw/telegram_messages"
    DEFAULT_MSG_LIMIT: int = 1000

class Settings(BaseSettings):
    """Environment-specific settings loaded from .env or environment variables."""
    # Credentials
    API_ID: str = "34593938"
    API_HASH: str = "0904e1590ff4c62a79155c96799dd50e"
    
    # Database configuration
    DB_USER: str = "birhanu"
    DB_PASS: str = "7121"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "medical_warehouse"

    # Attach constants
    PROJECT: ProjectConstants = ProjectConstants()

    @property
    def DATABASE_URL(self) -> str:
        """Computed property for SQLAlchemy/Alembic connections."""
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        # Pydantic looks for .env in the project root (two levels up)
        env_file = os.path.join(os.path.dirname(__file__), "../../.env")
        case_sensitive = True

# Instantiate for use across the warehouse scripts
settings = Settings()