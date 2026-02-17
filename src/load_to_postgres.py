import pandas as pd
import json
import os
import glob
import sys
from sqlalchemy import text, create_engine

# 1. Dynamically find the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import settings from your config file
from app.core.config import settings

class TelegramDataLoader:
    def __init__(self):
        """Initializes the loader and ensures the database exists."""
        self._ensure_database_exists()
        # Create the engine for the specific project database
        self.engine = create_engine(settings.DATABASE_URL)
        print(f"✅ Loader initialized for database: {settings.DB_NAME}")

    def _ensure_database_exists(self):
        """Connects to system 'postgres' DB to create target DB if missing."""
        # Build URL for the default postgres maintenance database
        sys_url = f"postgresql://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/postgres"
        
        # Isolation level AUTOCOMMIT is required for CREATE DATABASE
        temp_engine = create_engine(sys_url, isolation_level="AUTOCOMMIT")
        
        try:
            with temp_engine.connect() as conn:
                exists = conn.execute(
                    text(f"SELECT 1 FROM pg_database WHERE datname = '{settings.DB_NAME}'")
                ).scalar()
                
                if not exists:
                    print(f"🚀 Database '{settings.DB_NAME}' not found. Creating it now...")
                    conn.execute(text(f"CREATE DATABASE {settings.DB_NAME}"))
                else:
                    print(f"💎 Database '{settings.DB_NAME}' exists.")
        finally:
            temp_engine.dispose()

    def load_json_files(self, folder_path):
        """Reads JSON files from a folder and all subfolders."""
        all_messages = []
        search_pattern = os.path.join(folder_path, "**", "*.json")
        files = glob.glob(search_pattern, recursive=True)
        
        if not files:
            print(f"⚠️ No JSON files found in {folder_path}")
            return []

        for file_path in files:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_messages.extend(data)
                    else:
                        all_messages.append(data)
                except json.JSONDecodeError:
                    print(f"❌ Skipping invalid JSON: {file_path}")
                    
        print(f"📊 Read {len(all_messages)} records from local files.")
        return all_messages

    def upload_to_postgres(self, data, table_name, schema):
        """Uploads data to PostgreSQL and ensures the schema exists."""
        if not data:
            print("🛑 No data to upload.")
            return

        df = pd.DataFrame(data)
        
        try:
            # Ensure the specific schema (e.g., 'raw') exists
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                conn.commit()
                print(f"📂 Schema '{schema}' verified.")

            # Load data into the database
            df.to_sql(
                table_name, 
                con=self.engine, 
                schema=schema, 
                if_exists='append', 
                index=False
            )
            print(f"✅ Successfully loaded {len(df)} records into {schema}.{table_name}")
            
        except Exception as e:
            print(f"🔥 An error occurred during upload: {e}")

    def run_pipeline(self, folder_path, table_name="telegram_messages", schema="raw"):
        """Executes the full Load-and-Upload process."""
        data = self.load_json_files(folder_path)
        if data:
            self.upload_to_postgres(data, table_name, schema)

if __name__ == "__main__":
    loader = TelegramDataLoader()
    
    # We load to the 'raw' schema. Table name should be 'telegram_messages'
    # so that dbt can find it via your source.yml configuration.
    loader.run_pipeline(
        folder_path="../data/raw/telegram_messages", 
        table_name="telegram_messages", 
        schema="raw"
    )