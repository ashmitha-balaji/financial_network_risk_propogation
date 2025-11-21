"""
Flajolet-Martin Algorithm for Cardinality Estimation
Estimates number of distinct entities in financial network
"""

import mmh3
import math

class FlajoletMartin:
    """Approximate distinct counting algorithm"""
    
    def __init__(self, num_hash_functions=10):
        """
        Args:
            num_hash_functions: Number of hash functions for averaging
        """
        self.num_hash = num_hash_functions
        self.max_trailing_zeros = [0] * num_hash_functions
    
    def add(self, item):
        """Add item to cardinality estimator"""
        for i in range(self.num_hash):
            # Hash the item
            hash_value = mmh3.hash(str(item), seed=i)
            
            # Convert to binary and count trailing zeros
            binary = bin(hash_value & 0xFFFFFFFF)[2:]  # 32-bit positive
            trailing_zeros = len(binary) - len(binary.rstrip('0'))
            
            # Update max trailing zeros for this hash function
            self.max_trailing_zeros[i] = max(self.max_trailing_zeros[i], trailing_zeros)
    
    def estimate(self):
        """Estimate number of distinct elements"""
        # Average the estimates from all hash functions
        estimates = [2 ** z for z in self.max_trailing_zeros]
        
        # Use median for robustness
        estimates.sort()
        median_estimate = estimates[len(estimates) // 2]
        
        # Apply correction factor (φ ≈ 0.77351)
        phi = 0.77351
        return int(median_estimate / phi)
    
    def stats(self):
        """Get estimator statistics"""
        return {
            'num_hash_functions': self.num_hash,
            'max_trailing_zeros': self.max_trailing_zeros,
            'estimated_cardinality': self.estimate()
        }

if __name__ == "__main__":
    # Test Flajolet-Martin
    fm = FlajoletMartin(num_hash_functions=10)
    
    # Add distinct items
    items = [f"BANK_{i}" for i in range(100)]
    for item in items:
        fm.add(item)
    
    print("Flajolet-Martin Test:")
    print(f"  Actual distinct count: {len(set(items))}")
    print(f"  Estimated count: {fm.estimate()}")
    print(f"  Error: {abs(fm.estimate() - len(set(items))) / len(set(items)) * 100:.1f}%")
