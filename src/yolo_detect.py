import os
import pandas as pd
import glob
from ultralytics import YOLO

class YOLOAnalyzer:
    def __init__(self, model_name='yolov8n.pt'):
        """Initializes the YOLO model."""
        self.model = YOLO(model_name)
        print(f"YOLO model {model_name} initialized.")

    def detect_objects(self, image_dir):
        """Scans images and categorizes them based on detected objects."""
        results_list = []
        # Support both .jpg and .png
        image_files = glob.glob(os.path.join(image_dir, "**", "*.jpg"), recursive=True)
        
        if not image_files:
            print(f"No images found in {image_dir}")
            return None

        print(f"Starting detection on {len(image_files)} images...")

        for img_path in image_files:
            # Extract message_id from filename
            try:
                message_id = int(os.path.basename(img_path).split('.')[0])
            except ValueError:
                continue # Skip files that don't have numeric IDs
            
            # Run inference
            results = self.model(img_path, verbose=False)
            
            for r in results:
                # Extract detected labels and confidence scores
                names = [self.model.names[int(c)] for c in r.boxes.cls.tolist()]
                confs = r.boxes.conf.tolist()
                
                # Logic for Classification Scheme
                has_person = 'person' in names
                # Using 'bottle' and containers as proxies for medical products
                has_product = any(x in names for x in ['bottle', 'cup', 'bowl', 'vase']) 
                
                if has_person and has_product:
                    category = 'promotional'
                elif has_product:
                    category = 'product_display'
                elif has_person:
                    category = 'lifestyle'
                else:
                    category = 'other'

                results_list.append({
                    "message_id": message_id,
                    "detected_objects": ", ".join(names) if names else "none",
                    "confidence_score": round(max(confs), 4) if confs else 0.0,
                    "image_category": category,
                    "image_path": img_path
                })

        return pd.DataFrame(results_list)

    def save_results(self, df, output_path="../data/image_detections.csv"):
        """Saves the detection results to a CSV file."""
        if df is not None and not df.empty:
            df.to_csv(output_path, index=False)
            print(f"Detection results saved to {output_path}")
        else:
            print("No data to save.")

if __name__ == "__main__":
    analyzer = YOLOAnalyzer()
    # Adjust path to match your data structure from Task 1
    detections_df = analyzer.detect_objects("../data/raw/images")
    analyzer.save_results(detections_df)