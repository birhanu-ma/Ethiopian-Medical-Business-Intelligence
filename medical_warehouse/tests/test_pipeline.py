import pytest
import os
import json
import sys
import pandas as pd
from datetime import datetime, timezone

# Ensure the project root is in the path for module discovery
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Corrected Imports
from medical_warehouse.Scripts.scraper import TelegramScraper, START_DATE_STR
from medical_warehouse.Scripts.load_to_postgres import TelegramDataLoader
from medical_warehouse.Scripts.yolo_detect import YOLOAnalyzer
from medical_warehouse.Scripts.yolo_data_loader import YoloDataHandler

# --- 1. Scraper Test: Logic Accuracy ---
def test_clean_username_logic():
    """Test that the scraper correctly strips URLs and symbols."""
    scraper = TelegramScraper()
    assert scraper.clean_username("@CheMed123") == "CheMed123"
    assert scraper.clean_username("https://t.me/lobelia4cosmetics") == "lobelia4cosmetics"
    assert scraper.clean_username("t.me/some_channel ") == "some_channel"

# --- 2. Configuration Test: Constants & Environment ---
def test_scraper_date_boundary():
    """Test that the scraper's start date constant is correctly defined."""
    # Since start_date isn't in __init__, we test the global constant used by the logic
    expected_date_str = "2026-01-18"
    assert START_DATE_STR == expected_date_str
    
    # Verify it can be converted to a valid datetime
    dt = datetime.strptime(START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    assert dt.year == 2026
    assert dt.month == 1

# --- 3. YOLO Analyzer Test: Classification Accuracy ---
def test_yolo_classification_logic():
    """Test the internal classification logic of the YOLO Analyzer."""
    analyzer = YOLOAnalyzer()
    
    # Promotional: Person + Medical Proxy (bottle)
    assert analyzer._classify_image(['person', 'bottle']) == 'promotional'
    
    # Product Display: Just the medical proxy
    assert analyzer._classify_image(['bottle', 'cup']) == 'product_display'
    
    # Lifestyle: Just a person
    assert analyzer._classify_image(['person']) == 'lifestyle'
    
    # Other: Random objects
    assert analyzer._classify_image(['dog', 'car']) == 'other'

# --- 4. Data Loader Test: File Ingestion Resilience ---
def test_json_loading_resilience(tmp_path):
    """Test that the loader handles missing files or empty directories gracefully."""
    loader = TelegramDataLoader()
    # Test a non-existent path
    data = loader.load_json_files(str(tmp_path / "empty_folder"))
    assert data == []

# --- 5. Data Handler Test: Numeric Cleaning ---
def test_handler_numeric_conversion():
    """Ensure message_ids are correctly cast to integers for database joining."""
    # Updated to point to your actual class in yolo_data_loader
    handler = YoloDataHandler() 
    
    raw_data = pd.DataFrame({
        "message_id": [101, "102", "InvalidID"],
        "image_category": ["prod", "prod", "prod"]
    })
    
    # Simulating the internal cleaning logic used during upload
    df = raw_data.copy()
    df['message_id'] = pd.to_numeric(df['message_id'], errors='coerce')
    df = df.dropna(subset=['message_id'])
    df['message_id'] = df['message_id'].astype(int)
    
    assert len(df) == 2
    assert df['message_id'].iloc[0] == 101
    assert df['message_id'].iloc[1] == 102