"""
Entity Similarity Detection using LSH
Finds banks with similar connection patterns in financial network
"""

import pandas as pd
import sys
import os
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lsh.minhash_lsh import MinHashLSH

class EntitySimilarityDetector:
    """Detect similar entities in financial network using LSH"""
    
    def __init__(self, edges_path, nodes_path):
        self.edges_path = edges_path
        self.nodes_path = nodes_path
        self.lsh = MinHashLSH(num_hashes=100, num_bands=20)
        self.entity_connections = defaultdict(set)
    
    def build_connection_graph(self):
        """Build entity connection graph from edges"""
        print("📊 Building connection graph...")
        
        edges = pd.read_csv(self.edges_path)
        
        for _, edge in edges.iterrows():
            src = edge.get(':START_ID', edge.get('source'))
            dst = edge.get(':END_ID', edge.get('target'))
            
            # Add bidirectional connections
            self.entity_connections[src].add(dst)
            self.entity_connections[dst].add(src)
        
        print(f"  ✓ Built graph with {len(self.entity_connections)} entities")
    
    def index_entities(self):
        """Index all entities in LSH"""
        print("\n🔍 Indexing entities in LSH...")
        
        for entity_id, connections in self.entity_connections.items():
            if len(connections) > 0:  # Only index entities with connections
                self.lsh.add_entity(entity_id, connections)
        
        print(f"  ✓ Indexed {len(self.lsh.signatures)} entities")
    
    def find_similar_entities(self, threshold=0.5):
        """Find all similar entity pairs"""
        print(f"\n🔗 Finding similar entities (threshold={threshold})...")
        
        similar_pairs = self.lsh.find_all_similar_pairs(threshold)
        
        print(f"  ✓ Found {len(similar_pairs)} similar pairs")
        
        return similar_pairs
    
    def save_results(self, similar_pairs, output_path):
        """Save similarity results"""
        print(f"\n💾 Saving results to {output_path}...")
        
        results_df = pd.DataFrame(similar_pairs, columns=['entity1', 'entity2', 'similarity'])
        results_df = results_df.sort_values('similarity', ascending=False)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        results_df.to_csv(output_path, index=False)
        
        print(f"  ✓ Saved {len(results_df)} similar pairs")
        
        return results_df
    
    def print_top_similar(self, similar_pairs, top_n=10):
        """Print top similar entity pairs"""
        print(f"\n📋 Top {top_n} Most Similar Entity Pairs:")
        print("-" * 60)
        
        sorted_pairs = sorted(similar_pairs, key=lambda x: x[2], reverse=True)
        
        for i, (e1, e2, sim) in enumerate(sorted_pairs[:top_n], 1):
            print(f"{i:2d}. {e1:20} ↔ {e2:20} | Similarity: {sim:.3f}")

def main():
    print("="*60)
    print("🔗 LOCALITY SENSITIVE HASHING - ENTITY SIMILARITY")
    print("="*60)
    
    detector = EntitySimilarityDetector(
        edges_path="data/edges.csv",
        nodes_path="data/nodes.csv"
    )
    
    # Build and index
    detector.build_connection_graph()
    detector.index_entities()
    
    # Find similar entities
    similar_pairs = detector.find_similar_entities(threshold=0.3)
    
    # Print top similar
    detector.print_top_similar(similar_pairs, top_n=15)
    
    # Save results
    results_df = detector.save_results(similar_pairs, "output/lsh_similar_entities.csv")
    
    # Print LSH stats
    print(f"\n📊 LSH Statistics:")
    stats = detector.lsh.stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ LSH similarity detection complete!")

if __name__ == "__main__":
    main()
