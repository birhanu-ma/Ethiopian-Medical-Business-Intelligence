import os
import sys
import subprocess
from dagster import asset, Definitions, AssetSelection, ScheduleDefinition
from pathlib import Path

# Identify the project root directory
PROJECT_ROOT = Path(__file__).parent.absolute()
PYTHON_EXE = sys.executable  # Use the virtual environment's python

# 1. Scrape Telegram Data
@asset
def telegram_data_scraped():
    """Runs the Telegram scraper (src/scraper.py)."""
    script_path = PROJECT_ROOT / "src" / "scraper.py"
    subprocess.run(
        [PYTHON_EXE, str(script_path)], 
        check=True, 
        cwd=str(PROJECT_ROOT)
    )
    return "Raw data saved to data/raw/"

# 2. Load Raw Data to Postgres
@asset(deps=[telegram_data_scraped])
def raw_data_loaded_to_postgres():
    """Loads scraped JSON messages using src/load_to_postgres.py."""
    script_path = PROJECT_ROOT / "src" / "load_to_postgres.py"
    subprocess.run(
        [PYTHON_EXE, str(script_path)], 
        check=True, 
        cwd=str(PROJECT_ROOT)
    )
    return "Raw data now in Postgres"

# 3. Run YOLO Enrichment & Upload
@asset(deps=[telegram_data_scraped])
def yolo_enrichment_complete():
    """Runs YOLO detector and then the uploader script."""
    detector_script = PROJECT_ROOT / "src" / "yolo_detect.py"
    uploader_script = PROJECT_ROOT / "src" / "yolo_data_loader.py"
    
    # Run detector
    subprocess.run([PYTHON_EXE, str(detector_script)], check=True, cwd=str(PROJECT_ROOT))
    
    # Run uploader to Postgres
    subprocess.run([PYTHON_EXE, str(uploader_script)], check=True, cwd=str(PROJECT_ROOT))
    
    return "Image detections enriched and uploaded"

# 4. Run dbt Transformations
@asset(deps=[raw_data_loaded_to_postgres, yolo_enrichment_complete])
def dbt_warehouse_refresh():
    """Executes dbt run inside the medical_warehouse folder."""
    dbt_path = PROJECT_ROOT / "medical_warehouse"
    
    # Run dbt models
    subprocess.run(["dbt", "run"], check=True, cwd=str(dbt_path))
    
    return "dbt Staging and Marts built successfully"

# --- Orchestration Definitions ---

daily_schedule = ScheduleDefinition(
    name="daily_medical_pipeline",
    target=AssetSelection.all(),
    cron_schedule="0 0 * * *",
)

defs = Definitions(
    assets=[
        telegram_data_scraped, 
        raw_data_loaded_to_postgres, 
        yolo_enrichment_complete, 
        dbt_warehouse_refresh
    ],
    schedules=[daily_schedule]
)