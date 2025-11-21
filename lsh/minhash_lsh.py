"""
MinHash LSH for Finding Similar Entities
Uses LSH to find banks with similar connection patterns
"""

import mmh3
import random
from collections import defaultdict
import numpy as np

class MinHashLSH:
    """LSH using MinHash for Jaccard similarity"""
    
    def __init__(self, num_hashes=100, num_bands=20):
        """
        Args:
            num_hashes: Number of hash functions for MinHash
            num_bands: Number of bands for LSH
        """
        self.num_hashes = num_hashes
        self.num_bands = num_bands
        self.rows_per_band = num_hashes // num_bands
        
        # LSH buckets: {band_id: {hash: [entity_ids]}}
        self.buckets = defaultdict(lambda: defaultdict(list))
        
        # Entity signatures: {entity_id: signature}
        self.signatures = {}
        
        print(f"✅ MinHash LSH initialized:")
        print(f"   Hash functions: {num_hashes}")
        print(f"   Bands: {num_bands}")
        print(f"   Rows per band: {self.rows_per_band}")
    
    def _compute_minhash(self, shingles):
        """Compute MinHash signature for set of shingles"""
        signature = []
        
        for i in range(self.num_hashes):
            min_hash = float('inf')
            for shingle in shingles:
                hash_val = mmh3.hash(str(shingle), seed=i)
                min_hash = min(min_hash, hash_val)
            signature.append(min_hash)
        
        return signature
    
    def add_entity(self, entity_id, connections):
        """
        Add entity to LSH index
        
        Args:
            entity_id: Unique entity identifier
            connections: Set of connected entity IDs (shingles)
        """
        # Compute MinHash signature
        signature = self._compute_minhash(connections)
        self.signatures[entity_id] = signature
        
        # Add to LSH buckets (banding technique)
        for band_id in range(self.num_bands):
            start = band_id * self.rows_per_band
            end = start + self.rows_per_band
            band_signature = tuple(signature[start:end])
            
            # Hash the band signature
            band_hash = hash(band_signature)
            self.buckets[band_id][band_hash].append(entity_id)
    
    def find_similar(self, entity_id, threshold=0.5):
        """
        Find entities similar to given entity
        
        Args:
            entity_id: Entity to find similar entities for
            threshold: Jaccard similarity threshold (0-1)
        
        Returns:
            List of (similar_entity_id, estimated_similarity)
        """
        if entity_id not in self.signatures:
            return []
        
        signature = self.signatures[entity_id]
        candidates = set()
        
        # Find candidate pairs from LSH buckets
        for band_id in range(self.num_bands):
            start = band_id * self.rows_per_band
            end = start + self.rows_per_band
            band_signature = tuple(signature[start:end])
            band_hash = hash(band_signature)
            
            # Get all entities in same bucket
            candidates.update(self.buckets[band_id][band_hash])
        
        # Remove self
        candidates.discard(entity_id)
        
        # Compute actual Jaccard similarity for candidates
        similar = []
        for candidate in candidates:
            similarity = self._jaccard_similarity(
                self.signatures[entity_id],
                self.signatures[candidate]
            )
            
            if similarity >= threshold:
                similar.append((candidate, similarity))
        
        # Sort by similarity (descending)
        similar.sort(key=lambda x: x[1], reverse=True)
        
        return similar
    
    def _jaccard_similarity(self, sig1, sig2):
        """Estimate Jaccard similarity from MinHash signatures"""
        matches = sum(1 for a, b in zip(sig1, sig2) if a == b)
        return matches / len(sig1)
    
    def find_all_similar_pairs(self, threshold=0.5):
        """Find all pairs of similar entities"""
        all_pairs = []
        processed = set()
        
        for entity_id in self.signatures.keys():
            if entity_id in processed:
                continue
            
            similar = self.find_similar(entity_id, threshold)
            for sim_id, similarity in similar:
                if sim_id not in processed:
                    all_pairs.append((entity_id, sim_id, similarity))
            
            processed.add(entity_id)
        
        return all_pairs
    
    def stats(self):
        """Get LSH statistics"""
        bucket_sizes = []
        for band_buckets in self.buckets.values():
            bucket_sizes.extend(len(entities) for entities in band_buckets.values())
        
        return {
            'num_entities': len(self.signatures),
            'num_bands': self.num_bands,
            'num_hashes': self.num_hashes,
            'total_buckets': sum(len(b) for b in self.buckets.values()),
            'avg_bucket_size': np.mean(bucket_sizes) if bucket_sizes else 0,
            'max_bucket_size': max(bucket_sizes) if bucket_sizes else 0
        }

if __name__ == "__main__":
    # Test MinHash LSH
    lsh = MinHashLSH(num_hashes=100, num_bands=20)
    
    # Add sample entities with connections
    lsh.add_entity("BANK_JPM", {"BANK_BAC", "BANK_WFC", "BANK_C"})
    lsh.add_entity("BANK_BAC", {"BANK_JPM", "BANK_WFC", "BANK_GS"})
    lsh.add_entity("BANK_WFC", {"BANK_JPM", "BANK_BAC", "BANK_USB"})
    lsh.add_entity("BANK_USB", {"BANK_WFC", "BANK_PNC"})
    
    # Find similar entities
    similar = lsh.find_similar("BANK_JPM", threshold=0.3)
    print("\nSimilar to BANK_JPM:")
    for entity, sim in similar:
        print(f"  {entity}: {sim:.3f}")
    
    print(f"\nLSH Stats: {lsh.stats()}")
