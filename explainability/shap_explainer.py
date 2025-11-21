"""
SHAP (SHapley Additive exPlanations) for Model Explainability
Explains which features contribute to cascade predictions
"""

import pandas as pd
import numpy as np
import joblib
import shap
import matplotlib.pyplot as plt
import os

class ModelExplainer:
    """Explain cascade predictions using SHAP"""
    
    def __init__(self, model_path, features_path):
        print("📂 Loading model and data...")
        
        # Load model
        model_data = joblib.load(model_path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_columns = model_data['feature_columns']
        
        # Load features
        self.features = pd.read_csv(features_path)
        
        print(f"  ✓ Model loaded")
        print(f"  ✓ Features loaded: {len(self.features)} samples")
    
    def prepare_data(self):
        """Prepare data for SHAP"""
        X = self.features[self.feature_columns].values
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        X_scaled = self.scaler.transform(X)
        
        return X_scaled
    
    def compute_shap_values(self, X, sample_size=100):
        """Compute SHAP values for model explanations"""
        print("\n🔍 Computing SHAP values...")
        
        # Sample data for efficiency
        if len(X) > sample_size:
            indices = np.random.choice(len(X), sample_size, replace=False)
            X_sample = X[indices]
        else:
            X_sample = X
        
        # Create SHAP explainer
        explainer = shap.TreeExplainer(self.model)
        shap_values = explainer.shap_values(X_sample)
        
        # For binary classification, shap_values might be a list
        if isinstance(shap_values, list):
            shap_values = shap_values[1]  # Positive class
        
        print(f"  ✓ SHAP values computed for {len(X_sample)} samples")
        
        return shap_values, X_sample, explainer
    
    def plot_feature_importance(self, shap_values, X_sample, output_path):
        """Plot SHAP feature importance"""
        print("\n📊 Generating SHAP summary plot...")
        
        plt.figure(figsize=(10, 8))
        shap.summary_plot(
            shap_values, X_sample,
            feature_names=self.feature_columns,
            show=False
        )
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        print(f"  ✓ Plot saved to: {output_path}")
    
    def explain_top_predictions(self, shap_values, X_sample, n=5):
        """Explain top predictions"""
        print(f"\n💡 Explaining Top {n} Cascade Predictions:")
        print("-" * 80)
        
        # Get prediction probabilities
        probabilities = self.model.predict_proba(X_sample)[:, 1]
        
        # Get top predictions
        top_indices = np.argsort(probabilities)[-n:][::-1]
        
        for rank, idx in enumerate(top_indices, 1):
            prob = probabilities[idx]
            
            # Handle different SHAP value formats
            if len(shap_values.shape) == 2:
                shap_vals = shap_values[idx]
            else:
                shap_vals = shap_values[idx, :]
            
            print(f"\n{rank}. Prediction Probability: {prob:.3f}")
            
            # Top contributing features
            # Convert to numpy array if needed
            shap_vals_array = np.array(shap_vals).flatten()
            
            feature_contributions = list(zip(self.feature_columns, shap_vals_array))
            feature_contributions.sort(key=lambda x: abs(x[1]), reverse=True)
            
            print("   Top contributing features:")
            for feat, contrib in feature_contributions[:5]:
                direction = "↑" if contrib > 0 else "↓"
                print(f"     {direction} {feat:30} {contrib:+.4f}")
    
    def generate_explanation_report(self, shap_values, output_path):
        """Generate comprehensive explanation report"""
        print("\n📝 Generating explanation report...")
        
        # Compute mean absolute SHAP values
        if len(shap_values.shape) == 3:
            # Multi-output (shouldn't happen for binary classification)
            mean_abs_shap = np.abs(shap_values[:, :, 1]).mean(axis=0)
        else:
            mean_abs_shap = np.abs(shap_values).mean(axis=0)
        
        # Ensure it's 1D array
        if len(mean_abs_shap.shape) > 1:
            mean_abs_shap = mean_abs_shap.flatten()
        
        # Convert to Python dict with proper type conversion
        feature_importance_dict = {}
        for i, feat in enumerate(self.feature_columns):
            # Extract scalar value properly
            if hasattr(mean_abs_shap[i], 'item'):
                val = float(mean_abs_shap[i].item())
            elif isinstance(mean_abs_shap[i], np.ndarray):
                val = float(mean_abs_shap[i].flatten()[0])
            else:
                val = float(mean_abs_shap[i])
            
            feature_importance_dict[feat] = val
        
        # Create sorted list for top features
        top_features = sorted(
            [(feat, imp) for feat, imp in feature_importance_dict.items()],
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        report = {
            'global_feature_importance': feature_importance_dict,
            'top_features': top_features
        }
        
        import json
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"  ✓ Report saved to: {output_path}")

def main():
    print("="*60)
    print("🔍 SHAP MODEL EXPLAINABILITY ANALYSIS")
    print("="*60)
    
    # Find latest model
    model_dir = "output/models"
    models = [f for f in os.listdir(model_dir) if f.startswith('cascade_predictor_')]
    if not models:
        print("❌ No trained model found!")
        return
    
    latest_model = sorted(models)[-1]
    model_path = os.path.join(model_dir, latest_model)
    
    # Initialize explainer
    explainer = ModelExplainer(
        model_path=model_path,
        features_path="output/features/network_features.csv"
    )
    
    # Prepare data
    X = explainer.prepare_data()
    
    # Compute SHAP values
    shap_values, X_sample, shap_explainer = explainer.compute_shap_values(X, sample_size=100)
    
    # Plot feature importance
    explainer.plot_feature_importance(
        shap_values, X_sample,
        "output/explanations/shap_feature_importance.png"
    )
    
    # Explain top predictions
    explainer.explain_top_predictions(shap_values, X_sample, n=5)
    
    # Generate report
    explainer.generate_explanation_report(
        shap_values,
        "output/explanations/explanation_report.json"
    )
    
    print("\n✅ Explainability analysis complete!")

if __name__ == "__main__":
    main()
