"""
Reservoir Sampling for Random Edge Sampling
Maintains random sample of edges from infinite stream
"""

import random

class ReservoirSampler:
    """Uniform random sampling from data stream"""
    
    def __init__(self, k):
        """
        Args:
            k: Size of reservoir (sample size)
        """
        self.k = k
        self.reservoir = []
        self.count = 0
    
    def add(self, item):
        """Add item to reservoir"""
        self.count += 1
        
        if len(self.reservoir) < self.k:
            # Reservoir not full, add item
            self.reservoir.append(item)
        else:
            # Randomly replace with decreasing probability
            j = random.randint(1, self.count)
            if j <= self.k:
                self.reservoir[j - 1] = item
    
    def get_sample(self):
        """Get current reservoir sample"""
        return self.reservoir.copy()
    
    def stats(self):
        """Get sampler statistics"""
        return {
            'reservoir_size': self.k,
            'items_processed': self.count,
            'current_sample_size': len(self.reservoir),
            'sampling_probability': self.k / self.count if self.count > 0 else 0
        }

if __name__ == "__main__":
    # Test Reservoir Sampling
    rs = ReservoirSampler(k=10)
    
    # Process stream of 1000 items
    for i in range(1000):
        rs.add(f"edge_{i}")
    
    sample = rs.get_sample()
    print("Reservoir Sampling Test:")
    print(f"  Items processed: {rs.count}")
    print(f"  Sample size: {len(sample)}")
    print(f"  Sample (first 5): {sample[:5]}")
    print(f"  Stats: {rs.stats()}")
