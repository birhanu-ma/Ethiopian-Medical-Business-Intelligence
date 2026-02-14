import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic_settings import BaseSettings # <--- This fixes the NameError
from pathlib import Path

class Settings(BaseSettings):
    # Added these to prevent the "Extra inputs" validation error
    API_ID: str = ""
    API_HASH: str = ""
    
    # Database connection parameters
    DB_USER: str = "birhanu"
    DB_PASS: str = "7121"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "medical_warehouse"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    # Pydantic v2 configuration
    model_config = {
        "env_file": ".env",
        "extra": "ignore", # Ignores extra fields like API_ID if they cause issues
        "case_sensitive": True
    }

# Initialize engine and session
settings = Settings()
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db_engine():
    """Returns the database engine instance."""
    return engine