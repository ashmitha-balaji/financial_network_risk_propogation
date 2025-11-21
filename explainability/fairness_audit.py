"""
Fairness Audit for Financial ML Models
Checks for bias across different entity types and tiers
"""

import pandas as pd
import numpy as np
import joblib
import os
from collections import defaultdict

class FairnessAuditor:
    """Audit model for fairness across entity groups"""
    
    def __init__(self, model_path, features_path, predictions_path):
        print("📂 Loading data...")
        
        # Load model
        model_data = joblib.load(model_path)
        self.model = model_data['model']
        self.feature_columns = model_data['feature_columns']
        
        # Load data
        self.features = pd.read_csv(features_path)
        self.predictions = pd.read_csv(predictions_path)
        
        print(f"  ✓ Data loaded: {len(self.features)} samples")
    
    def audit_by_entity_type(self):
        """Audit fairness across entity types"""
        print("\n⚖️  Fairness Audit by Entity Type:")
        print("-" * 60)
        
        if 'node_type' not in self.predictions.columns:
            print("  ⚠️  'node_type' column not found")
            return {}
        
        results = {}
        
        for entity_type in self.predictions['node_type'].unique():
            subset = self.predictions[self.predictions['node_type'] == entity_type]
            
            results[entity_type] = {
                'count': len(subset),
                'predicted_cascade_rate': subset['cascade_prediction'].mean(),
                'avg_probability': subset['cascade_probability'].mean(),
                'high_risk_count': (subset['cascade_probability'] > 0.7).sum()
            }
            
            print(f"\n  {entity_type}:")
            print(f"    Entities: {results[entity_type]['count']}")
            print(f"    Predicted cascade rate: {results[entity_type]['predicted_cascade_rate']:.2%}")
            print(f"    Avg probability: {results[entity_type]['avg_probability']:.3f}")
            print(f"    High risk: {results[entity_type]['high_risk_count']}")
        
        return results
    
    def audit_by_tier(self):
        """Audit fairness across entity tiers"""
        print("\n⚖️  Fairness Audit by Tier:")
        print("-" * 60)
        
        if 'tier' not in self.predictions.columns:
            print("  ⚠️  'tier' column not found")
            return {}
        
        results = {}
        
        for tier in self.predictions['tier'].dropna().unique():
            subset = self.predictions[self.predictions['tier'] == tier]
            
            results[tier] = {
                'count': len(subset),
                'predicted_cascade_rate': subset['cascade_prediction'].mean(),
                'avg_probability': subset['cascade_probability'].mean()
            }
            
            print(f"\n  {tier}:")
            print(f"    Entities: {results[tier]['count']}")
            print(f"    Predicted cascade rate: {results[tier]['predicted_cascade_rate']:.2%}")
            print(f"    Avg probability: {results[tier]['avg_probability']:.3f}")
        
        return results
    
    def compute_disparate_impact(self, group_col='node_type', privileged_group=None):
        """
        Compute disparate impact ratio
        
        Disparate Impact = (Positive prediction rate for unprivileged) / 
                          (Positive prediction rate for privileged)
        
        Fair if ratio is between 0.8 and 1.25
        """
        print("\n📊 Disparate Impact Analysis:")
        print("-" * 60)
        
        if group_col not in self.predictions.columns:
            print(f"  ⚠️  '{group_col}' column not found")
            return {}
        
        groups = self.predictions[group_col].unique()
        positive_rates = {}
        
        for group in groups:
            subset = self.predictions[self.predictions[group_col] == group]
            positive_rate = subset['cascade_prediction'].mean()
            positive_rates[group] = positive_rate
        
        # If no privileged group specified, use group with highest rate
        if privileged_group is None:
            privileged_group = max(positive_rates, key=positive_rates.get)
        
        print(f"  Privileged group: {privileged_group}")
        print(f"  Positive rate: {positive_rates[privileged_group]:.3f}\n")
        
        disparate_impact = {}
        for group in groups:
            if group != privileged_group:
                di_ratio = positive_rates[group] / positive_rates[privileged_group]
                is_fair = 0.8 <= di_ratio <= 1.25
                
                disparate_impact[group] = {
                    'ratio': di_ratio,
                    'is_fair': is_fair
                }
                
                status = "✅ Fair" if is_fair else "⚠️  Potentially biased"
                print(f"  {group}:")
                print(f"    DI Ratio: {di_ratio:.3f} {status}")
                print(f"    Positive rate: {positive_rates[group]:.3f}")
        
        return disparate_impact
    
    def generate_fairness_report(self, output_path):
        """Generate comprehensive fairness report"""
        print("\n📝 Generating fairness report...")
        
        # Convert all numpy types to Python native types
        def convert_to_native(obj):
            """Convert numpy types to Python native types"""
            if isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native(item) for item in obj]
            elif hasattr(obj, 'item'):
                return obj.item()
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            else:
                return obj
        
        report = {
            'audit_by_entity_type': convert_to_native(self.audit_by_entity_type()),
            'audit_by_tier': convert_to_native(self.audit_by_tier()),
            'disparate_impact': convert_to_native(self.compute_disparate_impact())
        }
        
        import json
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"  ✓ Report saved to: {output_path}")

def main():
    print("="*60)
    print("⚖️  FAIRNESS AUDIT FOR CASCADE PREDICTOR")
    print("="*60)
    
    # Find latest files
    model_dir = "output/models"
    models = [f for f in os.listdir(model_dir) if f.startswith('cascade_predictor_')]
    if not models:
        print("❌ No trained model found!")
        return
    
    latest_model = sorted(models)[-1]
    model_path = os.path.join(model_dir, latest_model)
    
    # Initialize auditor
    auditor = FairnessAuditor(
        model_path=model_path,
        features_path="output/features/network_features.csv",
        predictions_path="output/predictions/cascade_predictions.csv"
    )
    
    # Run audits
    auditor.audit_by_entity_type()
    auditor.audit_by_tier()
    auditor.compute_disparate_impact()
    
    # Generate report
    auditor.generate_fairness_report("output/explanations/fairness_report.json")
    
    print("\n✅ Fairness audit complete!")

if __name__ == "__main__":
    main()
