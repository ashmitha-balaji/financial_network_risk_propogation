"""
Bloom Filter for Duplicate Edge Detection
Detects if an edge has been seen before in the stream
"""

import mmh3
from bitarray import bitarray

class BloomFilter:
    """Space-efficient probabilistic data structure for membership testing"""
    
    def __init__(self, size=10000, hash_count=5):
        """
        Args:
            size: Size of bit array
            hash_count: Number of hash functions
        """
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)
        self.items_added = 0
    
    def add(self, item):
        """Add item to filter"""
        for i in range(self.hash_count):
            digest = mmh3.hash(str(item), i) % self.size
            self.bit_array[digest] = 1
        self.items_added += 1
    
    def __contains__(self, item):
        """Check if item might be in set"""
        for i in range(self.hash_count):
            digest = mmh3.hash(str(item), i) % self.size
            if not self.bit_array[digest]:
                return False
        return True
    
    def false_positive_rate(self):
        """Estimate false positive probability"""
        m = self.size
        n = self.items_added
        k = self.hash_count
        
        if n == 0:
            return 0.0
        
        # (1 - e^(-kn/m))^k
        import math
        return (1 - math.exp(-k * n / m)) ** k
    
    def stats(self):
        """Get filter statistics"""
        return {
            'size': self.size,
            'hash_count': self.hash_count,
            'items_added': self.items_added,
            'bits_set': self.bit_array.count(1),
            'fill_rate': self.bit_array.count(1) / self.size,
            'estimated_fpr': self.false_positive_rate()
        }

if __name__ == "__main__":
    # Test Bloom Filter
    bf = BloomFilter(size=1000, hash_count=3)
    
    # Add some items
    edges = [("BANK_JPM", "BANK_BAC"), ("BANK_WFC", "BANK_C")]
    for edge in edges:
        bf.add(edge)
    
    # Test membership
    print("Testing Bloom Filter:")
    print(f"  ('BANK_JPM', 'BANK_BAC') in filter: {('BANK_JPM', 'BANK_BAC') in bf}")
    print(f"  ('BANK_XYZ', 'BANK_ABC') in filter: {('BANK_XYZ', 'BANK_ABC') in bf}")
    print(f"\nStats: {bf.stats()}")
