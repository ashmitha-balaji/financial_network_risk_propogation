"""
DGIM Algorithm for Counting 1s in Sliding Window
Estimates count of high-risk events in recent time window
"""

import math
from collections import deque

class DGIM:
    """DGIM algorithm for approximate counting in sliding windows"""
    
    def __init__(self, window_size):
        """
        Args:
            window_size: Size of sliding window
        """
        self.window_size = window_size
        self.buckets = deque()  # (size, timestamp)
        self.current_time = 0
    
    def update(self, bit):
        """Process new bit in stream"""
        self.current_time += 1
        
        # Remove expired buckets
        while self.buckets and self.buckets[0][1] < self.current_time - self.window_size:
            self.buckets.popleft()
        
        if bit == 1:
            # Add new bucket of size 1
            self.buckets.append((1, self.current_time))
            
            # Merge buckets if needed
            self._merge_buckets()
    
    def _merge_buckets(self):
        """Merge buckets to maintain DGIM property"""
        # Count buckets of each size
        size_counts = {}
        for size, _ in self.buckets:
            size_counts[size] = size_counts.get(size, 0) + 1
        
        # Merge if more than 2 buckets of same size
        for size in sorted(size_counts.keys()):
            if size_counts[size] > 2:
                # Find and merge oldest two buckets of this size
                merged = []
                to_merge = []
                
                for bucket in self.buckets:
                    if bucket[0] == size and len(to_merge) < 2:
                        to_merge.append(bucket)
                    else:
                        merged.append(bucket)
                
                if len(to_merge) == 2:
                    # Create merged bucket with size*2 and latest timestamp
                    new_size = size * 2
                    new_timestamp = max(to_merge[0][1], to_merge[1][1])
                    merged.append((new_size, new_timestamp))
                    merged.extend([b for b in self.buckets if b not in to_merge and b not in merged])
                
                self.buckets = deque(sorted(merged, key=lambda x: x[1]))
    
    def count(self):
        """Estimate count of 1s in window"""
        if not self.buckets:
            return 0
        
        # Sum all buckets except oldest, add half of oldest
        total = sum(size for size, _ in list(self.buckets)[1:])
        if self.buckets:
            total += self.buckets[0][0] // 2
        
        return total
    
    def stats(self):
        """Get algorithm statistics"""
        return {
            'window_size': self.window_size,
            'current_time': self.current_time,
            'num_buckets': len(self.buckets),
            'estimated_count': self.count(),
            'buckets': list(self.buckets)
        }

if __name__ == "__main__":
    # Test DGIM
    dgim = DGIM(window_size=10)
    
    # Process stream: 1,0,1,1,0,1,0,0,1,1
    stream = [1, 0, 1, 1, 0, 1, 0, 0, 1, 1]
    for bit in stream:
        dgim.update(bit)
    
    print("DGIM Test:")
    print(f"  Stream: {stream}")
    print(f"  Actual count: {sum(stream)}")
    print(f"  Estimated count: {dgim.count()}")
    print(f"  Stats: {dgim.stats()}")
