"""
Model Inference for Cascade Prediction
Load trained model and make predictions on new data
"""

import pandas as pd
import numpy as np
import joblib
import os

class CascadeInference:
    """Make predictions using trained cascade predictor"""
    
    def __init__(self, model_path):
        print(f"📂 Loading model from {model_path}...")
        
        model_data = joblib.load(model_path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_columns = model_data['feature_columns']
        
        print(f"  ✓ Model loaded successfully")
        print(f"  ✓ Features: {len(self.feature_columns)}")
    
    def predict(self, features_df):
        """
        Make cascade predictions
        
        Args:
            features_df: DataFrame with required features
        
        Returns:
            DataFrame with predictions and probabilities
        """
        print("\n🔮 Making predictions...")
        
        # Extract features
        X = features_df[self.feature_columns].values
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        # Scale
        X_scaled = self.scaler.transform(X)
        
        # Predict
        predictions = self.model.predict(X_scaled)
        probabilities = self.model.predict_proba(X_scaled)[:, 1]
        
        # Create results DataFrame
        results = features_df.copy()
        results['cascade_prediction'] = predictions
        results['cascade_probability'] = probabilities
        results['risk_level'] = pd.cut(
            probabilities,
            bins=[0, 0.3, 0.7, 1.0],
            labels=['Low', 'Medium', 'High']
        )
        
        print(f"  ✓ Predictions complete")
        print(f"  ✓ Predicted cascades: {predictions.sum()}")
        print(f"  ✓ High risk entities: {(probabilities > 0.7).sum()}")
        
        return results
    
    def identify_high_risk_entities(self, results, threshold=0.7):
        """Identify entities at high risk of causing cascades"""
        high_risk = results[results['cascade_probability'] > threshold].copy()
        high_risk = high_risk.sort_values('cascade_probability', ascending=False)
        
        return high_risk
    
    def save_predictions(self, results, output_path):
        """Save prediction results"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        results.to_csv(output_path, index=False)
        print(f"\n💾 Predictions saved to: {output_path}")

def main():
    print("="*60)
    print("🔮 CASCADE PREDICTION INFERENCE")
    print("="*60)
    
    # Find latest model
    model_dir = "output/models"
    models = [f for f in os.listdir(model_dir) if f.startswith('cascade_predictor_')]
    if not models:
        print("❌ No trained model found!")
        return
    
    latest_model = sorted(models)[-1]
    model_path = os.path.join(model_dir, latest_model)
    
    # Load model
    inference = CascadeInference(model_path)
    
    # Load features
    features = pd.read_csv("output/features/network_features.csv")
    
    # Make predictions
    results = inference.predict(features)
    
    # Identify high-risk entities
    high_risk = inference.identify_high_risk_entities(results, threshold=0.7)
    
    print("\n⚠️  HIGH RISK ENTITIES (Top 10):")
    print("-" * 80)
    
    display_cols = ['node_id:ID', 'cascade_probability', 'pagerank_score', 
                    'total_degree', 'total_exposure']
    available_cols = [col for col in display_cols if col in high_risk.columns]
    
    if 'node_id:ID' not in high_risk.columns and 'node_id' in high_risk.columns:
        high_risk['node_id:ID'] = high_risk['node_id']
    
    for i, row in high_risk.head(10).iterrows():
        node_id = row.get('node_id:ID', row.get('node_id', 'Unknown'))
        prob = row['cascade_probability']
        print(f"  {node_id:30} | Risk: {prob:.3f} {'🔴' if prob > 0.9 else '🟠'}")
    
    # Save predictions
    inference.save_predictions(results, "output/predictions/cascade_predictions.csv")
    
    print("\n✅ Inference complete!")

if __name__ == "__main__":
    main()
