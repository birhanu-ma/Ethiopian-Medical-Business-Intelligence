import asyncio
import os
import logging
import subprocess  # Added to run CLI commands like dbt
from medical_warehouse.Scripts.scraper import TelegramScraper
from medical_warehouse.Scripts.yolo_detect import YOLOAnalyzer
from medical_warehouse.Scripts.load_to_postgres import TelegramDataLoader
from medical_warehouse.Scripts.yolo_data_loader import YoloDataHandler
from medical_warehouse.Scripts.config import settings

async def run_full_pipeline():
    # ... (Phases 1 through 4 remain the same) ...

    # 5. Transform - Run dbt to clean and model the data
    print("\n--- Phase 5: Running dbt Transformations ---")
    try:
        # We assume your dbt project is in a folder named 'dbt_project'
        # Change 'cwd' to the actual path of your dbt folder
        dbt_path = os.path.join(os.getcwd(), "medical_dbt") 
        
        result = subprocess.run(
            ["dbt", "run"], 
            cwd=dbt_path, 
            capture_output=True, 
            text=True
        )
        
        if result.returncode == 0:
            print("✅ dbt transformations completed successfully!")
            print(result.stdout)
        else:
            print("❌ dbt failed!")
            print(result.stderr)
            
    except FileNotFoundError:
        print("⚠️ dbt command not found. Ensure dbt is installed in your .venv")

    print("\n🚀 Full Pipeline (ELT) Execution Complete!")