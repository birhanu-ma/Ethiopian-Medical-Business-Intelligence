import shap
import matplotlib.pyplot as plt
import pandas as pd
import joblib
import os
import numpy as np

def generate_model_explanations(model_path, X_test_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # Load model and data
    model = joblib.load(model_path)
    df = pd.read_csv(X_test_path)
    
    # Strictly define features to ensure alignment
    features = ['n_persons', 'n_bottles', 'n_pills', 'view_count']
    X_numeric = df[features]

    # 1. Initialize Explainer
    explainer = shap.TreeExplainer(model)
    
    # 2. Get SHAP values
    # We use the newer 'Explanation' object format which waterfall plots prefer
    explanation = explainer(X_numeric)

    # If it's a multiclass/binary model, we need to select class 1
    # explanation[:, :, 1] means: [all rows, all features, class 1]
    if len(explanation.shape) == 3:
        exp_to_plot = explanation[0, :, 1]  # Select first row, class 1
        shap_values_global = explanation.values[:, :, 1]
    else:
        exp_to_plot = explanation[0, :]     # Single output model
        shap_values_global = explanation.values

    # --- 3. Global Summary Plot ---
    plt.figure(figsize=(10, 6))
    shap.summary_plot(shap_values_global, X_numeric, show=False)
    plt.savefig(os.path.join(output_dir, "shap_summary_plot.png"), bbox_inches='tight')
    plt.close()

    # --- 4. Local Waterfall Plot (REPLACED Force Plot for Stability) ---
    plt.figure(figsize=(10, 6))
    # Waterfall plot automatically handles base_value + shap_values + feature_names
    shap.plots.waterfall(exp_to_plot, show=False)
    
    plt.savefig(os.path.join(output_dir, "shap_local_prediction.png"), bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    from medical_warehouse.Scripts.config import settings
    MODEL = os.path.join(settings.PROJECT.BASE_DATA_DIR, "models", "yolo_classifier.joblib")
    DATA = os.path.join(settings.PROJECT.BASE_DATA_DIR, "raw", "processed_data.csv")
    OUT = os.path.join(settings.PROJECT.BASE_DATA_DIR, "results")

    print("📊 Generating SHAP Waterfall and Summary plots...")
    if os.path.exists(MODEL) and os.path.exists(DATA):
        generate_model_explanations(MODEL, DATA, OUT)
        print(f"✅ Success! Results in {OUT}")
    else:
        print("❌ Error: Missing files. Run temp_setup.py first.")