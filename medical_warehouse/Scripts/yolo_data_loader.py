import pandas as pd
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text # Added text import

class YoloDataHandler:
    def __init__(self, engine=None):
        if engine:
            self.engine = engine
        else:
            current_dir = Path(__file__).resolve().parent if "__file__" in locals() else Path(os.getcwd())
            project_root = current_dir.parent
            
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            
            try:
                from app.db.database import engine as db_engine
                self.engine = db_engine
            except ImportError as e:
                try:
                    from app.core.config import settings
                    self.engine = create_engine(settings.DATABASE_URL)
                except Exception:
                    raise ImportError(
                        f"Could not find database configuration. "
                        f"Ensure 'app/db/session.py' exists. Error: {e}"
                    )
            
        print(f"Connected to database: {self.engine.url.database}")

    def upload_yolo_csv(self, csv_path, table_name='image_analysis', schema='processed'):
        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
            return

        df = pd.read_csv(csv_path)
        
        # Clean data: ensure message_id is numeric for database joining
        df['message_id'] = pd.to_numeric(df['message_id'], errors='coerce')
        df = df.dropna(subset=['message_id']).copy()
        df['message_id'] = df['message_id'].astype(int)
        
        # --- FIX STARTS HERE ---
        try:
            with self.engine.connect() as conn:
                # Create the schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                conn.commit()
                print(f"Ensured schema '{schema}' exists.")

            # Upload to Postgres
            df.to_sql(table_name, con=self.engine, schema=schema, if_exists='replace', index=False)
            print(f"Successfully uploaded {len(df)} rows to {schema}.{table_name}")
        except Exception as e:
            print(f"An error occurred during upload: {e}")
        # --- FIX ENDS HERE ---

if __name__ == "__main__":
    handler = YoloDataHandler()
    my_csv_path = "../data/image_detections.csv"
    handler.upload_yolo_csv(csv_path=my_csv_path, table_name='image_analysis', schema='processed')