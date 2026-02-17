import os
import glob
import logging
from typing import List, Optional, Dict, Any

import pandas as pd
from ultralytics import YOLO
from .config import settings

# --- Constants for Engineering Excellence ---
DEFAULT_MODEL: str = 'yolov8n.pt'
CATEGORY_PROMOTIONAL: str = 'promotional'
CATEGORY_PRODUCT: str = 'product_display'
CATEGORY_LIFESTYLE: str = 'lifestyle'
CATEGORY_OTHER: str = 'other'

class YOLOAnalyzer:
    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        """Initializes the YOLO model with explicit type hints."""
        self.model = YOLO(model_name)
        self._setup_logging()
        logging.info(f"YOLO model {model_name} initialized.")

    def _setup_logging(self) -> None:
        """Standardized logging to avoid cluttering the console."""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def _classify_image(self, names: List[str]) -> str:
        """Utility logic to determine the image category based on detections."""
        has_person = 'person' in names
        # COCO classes used as proxies for medical products
        medical_proxies = ['bottle', 'cup', 'bowl', 'vase']
        has_product = any(item in names for item in medical_proxies)
        
        if has_person and has_product:
            return CATEGORY_PROMOTIONAL
        elif has_product:
            return CATEGORY_PRODUCT
        elif has_person:
            return CATEGORY_LIFESTYLE
        return CATEGORY_OTHER

    def detect_objects(self, image_dir: str) -> Optional[pd.DataFrame]:
        """Scans directories for images and performs object detection."""
        results_list: List[Dict[str, Any]] = []
        
        # Build search pattern using glob for nested channel folders
        search_pattern = os.path.join(image_dir, "**", "*.jpg")
        image_files = glob.glob(search_pattern, recursive=True)
        
        if not image_files:
            logging.warning(f"⚠️ No images found in directory: {image_dir}")
            return None

        logging.info(f"🔍 Starting detection on {len(image_files)} images...")

        for img_path in image_files:
            try:
                # Extracts numeric message_id from filename (e.g., '123.jpg' -> 123)
                message_id = int(os.path.basename(img_path).split('.')[0])
            except (ValueError, IndexError):
                continue 
            
            # Run YOLO inference
            results = self.model(img_path, verbose=False)
            
            for r in results:
                # Map class indices to human-readable names
                names = [self.model.names[int(c)] for c in r.boxes.cls.tolist()]
                confs = r.boxes.conf.tolist()
                
                category = self._classify_image(names)

                results_list.append({
                    "message_id": message_id,
                    "detected_objects": ", ".join(names) if names else "none",
                    "confidence_score": round(max(confs), 4) if confs else 0.0,
                    "image_category": category,
                    "image_path": img_path
                })

        return pd.DataFrame(results_list)

    def save_results(self, df: pd.DataFrame, filename: str = "image_detections.csv") -> None:
        """Saves the detection results to the project's data directory."""
        if df is not None and not df.empty:
            output_path = os.path.join(settings.PROJECT.BASE_DATA_DIR, filename)
            df.to_csv(output_path, index=False)
            logging.info(f"✅ Detection results saved to: {output_path}")
        else:
            logging.warning("No detection data to save.")

# Allows running as a standalone script for testing
if __name__ == "__main__":
    analyzer = YOLOAnalyzer()
    raw_images = os.path.join(settings.PROJECT.BASE_DATA_DIR, settings.PROJECT.IMAGE_SUBDIR)
    results = analyzer.detect_objects(raw_images)
    analyzer.save_results(results)