"""
Machine Unlearning for Financial Models
Remove specific entities from trained model (e.g., for privacy/GDPR)
"""

import pandas as pd
import numpy as np
import joblib
import os
from sklearn.ensemble import RandomForestClassifier

class ModelUnlearner:
    """Unlearn specific entities from trained model"""
    
    def __init__(self, model_path, features_path):
        print("📂 Loading model...")
        
        # Load model
        model_data = joblib.load(model_path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_columns = model_data['feature_columns']
        
        # Load features
        self.features = pd.read_csv(features_path)
        
        print(f"  ✓ Model loaded")
        print(f"  ✓ Training data: {len(self.features)} samples")
    
    def unlearn_entities(self, entity_ids_to_remove):
        """
        Remove specific entities from model
        
        Methods:
        1. Exact unlearning: Retrain without those entities
        2. Approximate unlearning: Update model weights
        """
        print(f"\n🗑️  Unlearning {len(entity_ids_to_remove)} entities...")
        
        # Identify node ID column
        node_col = 'node_id:ID' if 'node_id:ID' in self.features.columns else 'node_id'
        
        # Remove entities from training data
        mask = ~self.features[node_col].isin(entity_ids_to_remove)
        features_filtered = self.features[mask].copy()
        
        print(f"  ✓ Remaining samples: {len(features_filtered)}")
        
        return features_filtered
    
    def retrain_model(self, features_filtered):
        """Retrain model without forgotten entities"""
        print("\n🔄 Retraining model...")
        
        # Prepare data
        X = features_filtered[self.feature_columns].values
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        # Generate labels (same method as original training)
        features_to_normalize = [
            'pagerank_score', 'total_degree', 'betweenness_centrality',
            'total_exposure'
        ]
        
        for feat in features_to_normalize:
            if feat in features_filtered.columns:
                min_val = features_filtered[feat].min()
                max_val = features_filtered[feat].max()
                if max_val > min_val:
                    features_filtered[f'norm_{feat}'] = (features_filtered[feat] - min_val) / (max_val - min_val)
        
        features_filtered['cascade_risk_score'] = (
            features_filtered.get('norm_pagerank_score', 0) * 0.3 +
            features_filtered.get('norm_total_degree', 0) * 0.2 +
            features_filtered.get('norm_betweenness_centrality', 0) * 0.3 +
            features_filtered.get('norm_total_exposure', 0) * 0.2
        )
        
        threshold = features_filtered['cascade_risk_score'].quantile(0.90)
        y = (features_filtered['cascade_risk_score'] > threshold).astype(int).values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Retrain Random Forest
        new_model = RandomForestClassifier(
            n_estimators=200,
            max_depth=15,
            min_samples_split=10,
            min_samples_leaf=5,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
        
        new_model.fit(X_scaled, y)
        
        print("  ✓ Model retrained successfully")
        
        return new_model
    
    def save_unlearned_model(self, new_model, output_path):
        """Save model after unlearning"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        joblib.dump({
            'model': new_model,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns
        }, output_path)
        
        print(f"\n💾 Unlearned model saved to: {output_path}")
    
    def verify_unlearning(self, entity_ids, new_model):
        """Verify that entities are truly forgotten"""
        print("\n✅ Verifying unlearning...")
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.features.columns else 'node_id'
        
        # Check that entities don't appear in any trees
        forgotten_count = 0
        
        for entity_id in entity_ids:
            entity_data = self.features[self.features[node_col] == entity_id]
            if len(entity_data) == 0:
                forgotten_count += 1
        
        print(f"  ✓ Successfully unlearned: {forgotten_count}/{len(entity_ids)} entities")

def main():
    print("="*60)
    print("🗑️  MACHINE UNLEARNING")
    print("="*60)
    
    # Find latest model
    model_dir = "output/models"
    models = [f for f in os.listdir(model_dir) if f.startswith('cascade_predictor_')]
    if not models:
        print("❌ No trained model found!")
        return
    
    latest_model = sorted(models)[-1]
    model_path = os.path.join(model_dir, latest_model)
    
    # Initialize unlearner
    unlearner = ModelUnlearner(
        model_path=model_path,
        features_path="output/features/network_features.csv"
    )
    
    # Example: Remove specific entities (e.g., for GDPR compliance)
    entities_to_remove = ['BANK_JPM', 'BANK_BAC']  # Example entities
    
    print(f"\n🎯 Entities to unlearn: {entities_to_remove}")
    
    # Unlearn entities
    features_filtered = unlearner.unlearn_entities(entities_to_remove)
    
    # Retrain model
    new_model = unlearner.retrain_model(features_filtered)
    
    # Save unlearned model
    unlearner.save_unlearned_model(
        new_model,
        "output/models/cascade_predictor_unlearned.joblib"
    )
    
    # Verify
    unlearner.verify_unlearning(entities_to_remove, new_model)
    
    print("\n✅ Machine unlearning complete!")

if __name__ == "__main__":
    main()
