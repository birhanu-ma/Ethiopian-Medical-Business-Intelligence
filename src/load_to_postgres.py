import pandas as pd
import json
import os
import glob
import sys
import os
# 1. Dynamically find the project root (Medical-Data-Scraper)
# This goes up one level from src/ to the root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# 2. Add the project root to sys.path so 'api' can be found
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# 3. Now this import will work
from app.api.database import get_db_engine

class TelegramDataLoader:
    def __init__(self):
        """Initializes the loader using the central database engine."""
        self.engine = get_db_engine()
        print(f"Loader initialized with engine: {self.engine.url.database}")

    def load_json_files(self, folder_path):
        """Reads JSON files from a folder and all subfolders."""
        all_messages = []
        search_pattern = os.path.join(folder_path, "**", "*.json")
        files = glob.glob(search_pattern, recursive=True)
        
        for file_path in files:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_messages.extend(data)
                    else:
                        all_messages.append(data)
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {file_path}")
                    
        print(f"Successfully read {len(all_messages)} records.")
        return all_messages

    def upload_to_postgres(self, data, table_name, schema='raw'):
        """Uploads data to PostgreSQL."""
        if not data:
            print("No data provided for upload.")
            return

        df = pd.DataFrame(data)
        try:
            df.to_sql(
                table_name, 
                con=self.engine, 
                schema=schema, 
                if_exists='append', 
                index=False
            )
            print(f"Successfully loaded {len(df)} records into {schema}.{table_name}")
        except Exception as e:
            print(f"An error occurred during upload: {e}")

    def run_pipeline(self, folder_path, table_name, schema='raw'):
        data = self.load_json_files(folder_path)
        self.upload_to_postgres(data, table_name, schema)

if __name__ == "__main__":
    # Example usage
    loader = TelegramDataLoader()
    loader.run_pipeline("../data/raw/telegram_messages", "fct_messages")