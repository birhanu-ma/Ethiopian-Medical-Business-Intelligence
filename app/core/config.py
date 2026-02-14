from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Credentials wrapped in quotes to prevent SyntaxErrors
    API_ID: str = "34593938"
    API_HASH: str = "0904e1590ff4c62a79155c96799dd50e"
    
    # Database connection parameters
    DB_USER: str = "birhanu"
    DB_PASS: str = "7121"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "medical_warehouse"

    # Computed property for SQLAlchemy
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        case_sensitive = True

settings = Settings()