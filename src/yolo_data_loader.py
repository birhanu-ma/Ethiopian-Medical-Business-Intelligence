import pandas as pd
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine

class YoloDataHandler:
    def __init__(self, engine=None):
        if engine:
            self.engine = engine
        else:
            # Step 1: Add the project root to sys.path
            # Since this file is in src/, go up one level to Medical-Data-Scraper
            current_dir = Path(__file__).resolve().parent if "__file__" in locals() else Path(os.getcwd())
            project_root = current_dir.parent
            
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            
            try:
                # Based on your file structure, the engine is in app/db/session.py
                from app.db.database import engine as db_engine
                self.engine = db_engine
            except ImportError as e:
                # Fallback: If you have a core config, use that to build the engine
                try:
                    from app.core.config import settings
                    self.engine = create_engine(settings.DATABASE_URL)
                except Exception:
                    raise ImportError(
                        f"Could not find database configuration. "
                        f"Ensure 'app/db/session.py' exists. Error: {e}"
                    )
            
        print(f"Connected to database: {self.engine.url.database}")

    def upload_yolo_csv(self, csv_path, table_name='image_analysis', schema='public'):
        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
            return

        df = pd.read_csv(csv_path)
        
        # Clean data: ensure message_id is numeric for database joining
        df['message_id'] = pd.to_numeric(df['message_id'], errors='coerce')
        df = df.dropna(subset=['message_id']).copy()
        df['message_id'] = df['message_id'].astype(int)
        
        # Upload to Postgres
        df.to_sql(table_name, con=self.engine, schema=schema, if_exists='replace', index=False)
        print(f"Successfully uploaded {len(df)} rows to {schema}.{table_name}")