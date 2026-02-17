import pandas as pd
import joblib
import os
from sklearn.ensemble import RandomForestClassifier
import numpy as np

os.makedirs("data/models", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

def setup():
    # Create Data with 'channel_name'
    data = pd.DataFrame({
        'message_id': range(100),
        'channel_name': np.random.choice(['CheMed123', 'lobelia4cosmetics', 'tikvahpharma'], 100),
        'n_persons': np.random.randint(0, 3, 100),
        'n_bottles': np.random.randint(0, 5, 100),
        'n_pills': np.random.randint(0, 10, 100),
        'view_count': np.random.randint(100, 5000, 100),
        'label': np.random.randint(0, 2, 100) 
    })
    data.to_csv("data/raw/processed_data.csv", index=False)

    # Train Model
    features = ['n_persons', 'n_bottles', 'n_pills', 'view_count']
    X = data[features]
    y = data['label']
    model = RandomForestClassifier(n_estimators=10, random_state=42).fit(X, y)
    
    joblib.dump(model, "data/models/yolo_classifier.joblib")
    print("✅ Mock environment ready.")

if __name__ == "__main__":
    setup()