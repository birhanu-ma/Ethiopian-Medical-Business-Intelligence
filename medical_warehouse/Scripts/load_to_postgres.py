import os
import json
import glob
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from sqlalchemy import text, create_engine, Engine
from .config import settings

# --- Constants for Engineering Excellence ---
DB_AUTOCOMMIT_LEVEL: str = "AUTOCOMMIT"
JSON_SEARCH_PATTERN: str = "**/*.json"

class TelegramDataLoader:
    def __init__(self) -> None:
        """Initializes the loader and ensures the database exists."""
        self._setup_logging()
        self._ensure_database_exists()
        
        # Create engine using the dataclass settings
        self.engine: Engine = create_engine(settings.DATABASE_URL)
        logging.info(f"Loader initialized for database: {settings.DB_NAME}")

    def _setup_logging(self) -> None:
        """Extracts reusable logging logic into a utility function."""
        log_dir = os.path.join(settings.PROJECT.BASE_DATA_DIR, "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def _ensure_database_exists(self) -> None:
        """Connects to system 'postgres' DB to create target DB if missing."""
        sys_url = f"postgresql://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/postgres"
        temp_engine = create_engine(sys_url, isolation_level=DB_AUTOCOMMIT_LEVEL)
        
        try:
            with temp_engine.connect() as conn:
                query = text("SELECT 1 FROM pg_database WHERE datname = :db_name")
                exists = conn.execute(query, {"db_name": settings.DB_NAME}).scalar()
                
                if not exists:
                    logging.info(f"🚀 Database '{settings.DB_NAME}' not found. Creating it...")
                    conn.execute(text(f"CREATE DATABASE {settings.DB_NAME}"))
                else:
                    logging.info(f"💎 Database '{settings.DB_NAME}' exists.")
        finally:
            temp_engine.dispose()

    def load_json_files(self, folder_path: str) -> List[Dict[str, Any]]:
        """Reads JSON files from a folder and all subfolders with type hints."""
        all_messages: List[Dict[str, Any]] = []
        search_pattern = os.path.join(folder_path, JSON_SEARCH_PATTERN)
        files = glob.glob(search_pattern, recursive=True)
        
        if not files:
            logging.warning(f"⚠️ No JSON files found in {folder_path}")
            return []

        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_messages.extend(data)
                    else:
                        all_messages.append(data)
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"❌ Skipping invalid file {file_path}: {e}")
                    
        logging.info(f"📊 Read {len(all_messages)} records from local files.")
        return all_messages

    def upload_to_postgres(self, data: List[Dict[str, Any]], table_name: str, schema: str) -> None:
        """Uploads data to PostgreSQL and ensures the schema exists."""
        if not data:
            logging.warning("🛑 No data to upload.")
            return

        df = pd.DataFrame(data)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                conn.commit()
                logging.info(f"📂 Schema '{schema}' verified.")

            df.to_sql(
                table_name, 
                con=self.engine, 
                schema=schema, 
                if_exists='append', 
                index=False
            )
            logging.info(f"✅ Loaded {len(df)} records into {schema}.{table_name}")
            
        except Exception as e:
            logging.error(f"🔥 Upload failed: {e}")

    def run_pipeline(self) -> None:
        """Executes the full process using the ProjectConstants dataclass."""
        # Dynamically build path from config
        source_path = os.path.join(
            settings.PROJECT.BASE_DATA_DIR, 
            settings.PROJECT.JSON_SUBDIR
        )
        
        data = self.load_json_files(source_path)
        if data:
            self.upload_to_postgres(
                data=data, 
                table_name=settings.PROJECT.MSG_TABLE, 
                schema=settings.PROJECT.RAW_SCHEMA
            )

if __name__ == "__main__":
    loader = TelegramDataLoader()
    loader.run_pipeline()