"""
Complete Streaming Pipeline
Processes edges.csv as a stream and applies all algorithms
"""

import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming_algorithms.bloom_filter import BloomFilter
from streaming_algorithms.dgim import DGIM
from streaming_algorithms.flajolet_martin import FlajoletMartin
from streaming_algorithms.reservoir_sampling import ReservoirSampler

class StreamingPipeline:
    """Process financial network edge stream"""
    
    def __init__(self, edges_path):
        self.edges_path = edges_path
        
        # Initialize streaming algorithms
        self.bloom_filter = BloomFilter(size=10000, hash_count=5)
        self.dgim = DGIM(window_size=1000)
        self.flajolet_martin_src = FlajoletMartin(num_hash_functions=10)
        self.flajolet_martin_dst = FlajoletMartin(num_hash_functions=10)
        self.reservoir = ReservoirSampler(k=100)
        
        self.edges_processed = 0
        self.duplicates_detected = 0
        self.high_risk_edges = 0
    
    def process_stream(self):
        """Process edges as stream"""
        print("="*60)
        print("🌊 STREAMING ALGORITHMS PIPELINE")
        print("="*60)
        
        print(f"\n📂 Loading edges from {self.edges_path}...")
        
        # Read edges in chunks (streaming simulation)
        chunk_size = 1000
        chunks_processed = 0
        
        for chunk in pd.read_csv(self.edges_path, chunksize=chunk_size):
            chunks_processed += 1
            
            for _, edge in chunk.iterrows():
                self._process_edge(edge)
            
            if chunks_processed % 5 == 0:
                print(f"  Processed {self.edges_processed} edges...")
        
        print(f"\n✅ Stream processing complete!")
        print(f"   Total edges: {self.edges_processed}")
    
    def _process_edge(self, edge):
        """Process single edge through all algorithms"""
        self.edges_processed += 1
        
        # Extract edge data
        src = edge.get(':START_ID', edge.get('source', 'unknown'))
        dst = edge.get(':END_ID', edge.get('target', 'unknown'))
        edge_type = edge.get(':TYPE', edge.get('type', 'unknown'))
        weight = edge.get('weight', 0)
        
        edge_id = f"{src}-{dst}"
        
        # 1. Bloom Filter: Check for duplicates
        if edge_id in self.bloom_filter:
            self.duplicates_detected += 1
        else:
            self.bloom_filter.add(edge_id)
        
        # 2. DGIM: Track high-risk edges (weight > threshold)
        threshold = 1000000000  # $1B
        is_high_risk = 1 if weight > threshold else 0
        self.dgim.update(is_high_risk)
        if is_high_risk:
            self.high_risk_edges += 1
        
        # 3. Flajolet-Martin: Count distinct sources and destinations
        self.flajolet_martin_src.add(src)
        self.flajolet_martin_dst.add(dst)
        
        # 4. Reservoir Sampling: Maintain random sample
        self.reservoir.add({
            'src': src,
            'dst': dst,
            'type': edge_type,
            'weight': weight
        })
    
    def print_results(self):
        """Print results from all algorithms"""
        print("\n" + "="*60)
        print("📊 STREAMING ALGORITHMS RESULTS")
        print("="*60)
        
        print("\n🔍 Bloom Filter:")
        bf_stats = self.bloom_filter.stats()
        print(f"   Items added: {bf_stats['items_added']}")
        print(f"   Duplicates detected: {self.duplicates_detected}")
        print(f"   Fill rate: {bf_stats['fill_rate']:.2%}")
        print(f"   Estimated FPR: {bf_stats['estimated_fpr']:.4f}")
        
        print("\n📈 DGIM (High-Risk Edges in Window):")
        dgim_stats = self.dgim.stats()
        print(f"   Window size: {dgim_stats['window_size']}")
        print(f"   Estimated high-risk in window: {dgim_stats['estimated_count']}")
        print(f"   Actual high-risk (total): {self.high_risk_edges}")
        
        print("\n🔢 Flajolet-Martin (Cardinality Estimation):")
        fm_src_estimate = self.flajolet_martin_src.estimate()
        fm_dst_estimate = self.flajolet_martin_dst.estimate()
        print(f"   Distinct source entities: ~{fm_src_estimate}")
        print(f"   Distinct destination entities: ~{fm_dst_estimate}")
        
        print("\n🎲 Reservoir Sampling:")
        rs_stats = self.reservoir.stats()
        print(f"   Sample size: {rs_stats['current_sample_size']}")
        print(f"   Sampling probability: {rs_stats['sampling_probability']:.4f}")
        
        sample = self.reservoir.get_sample()[:3]
        print(f"   Sample edges (first 3):")
        for edge in sample:
            print(f"     {edge['src']} -> {edge['dst']} ({edge['type']})")
    
    def save_results(self, output_path):
        """Save streaming results"""
        results = {
            'bloom_filter': self.bloom_filter.stats(),
            'dgim': self.dgim.stats(),
            'flajolet_martin_src': self.flajolet_martin_src.stats(),
            'flajolet_martin_dst': self.flajolet_martin_dst.stats(),
            'reservoir': self.reservoir.stats(),
            'summary': {
                'edges_processed': self.edges_processed,
                'duplicates_detected': self.duplicates_detected,
                'high_risk_edges': self.high_risk_edges
            }
        }
        
        import json
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to: {output_path}")

def main():
    edges_path = "data/edges.csv"
    output_path = "output/streaming_results.json"
    
    pipeline = StreamingPipeline(edges_path)
    pipeline.process_stream()
    pipeline.print_results()
    pipeline.save_results(output_path)

if __name__ == "__main__":
    main()
