from pydantic_settings import BaseSettings
from dataclasses import dataclass

@dataclass(frozen=True)
class ProjectConstants:
    RAW_SCHEMA: str = "raw"
    PROCESSED_SCHEMA: str = "processed"
    MSG_TABLE: str = "telegram_messages"
    ANALYSIS_TABLE: str = "image_analysis"
    BASE_DATA_DIR: str = "../data"
    IMAGE_SUBDIR: str = "raw/images"
    JSON_SUBDIR: str = "raw/telegram_messages"

class Settings(BaseSettings):
    API_ID: str = "34593938"
    API_HASH: str = "0904e1590ff4c62a79155c96799dd50e"
    
    DB_USER: str = "birhanu"
    DB_PASS: str = "7121"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "medical_warehouse"

    # --- THE FIX IS HERE ---
    # You must provide the type hint 'ProjectConstants' 
    # so Pydantic includes it in the object attributes.
    PROJECT: ProjectConstants = ProjectConstants()

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        case_sensitive = True

settings = Settings()