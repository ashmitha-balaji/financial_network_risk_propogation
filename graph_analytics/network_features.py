"""
Network Feature Extraction for ML
Extracts graph-based features: PageRank, centrality, clustering
"""

import pandas as pd
import numpy as np
from collections import defaultdict, deque
import os

class NetworkFeatureExtractor:
    """Extract ML features from financial network graph"""
    
    def __init__(self, nodes_path, edges_path):
        print("📂 Loading network data...")
        self.nodes = pd.read_csv(nodes_path)
        self.edges = pd.read_csv(edges_path)
        
        print(f"  ✓ Loaded {len(self.nodes)} nodes")
        print(f"  ✓ Loaded {len(self.edges)} edges")
        
        # Build adjacency lists
        self.out_edges = defaultdict(list)
        self.in_edges = defaultdict(list)
        self.edge_weights = {}
        
        self._build_graph()
    
    def _build_graph(self):
        """Build graph structure from edges"""
        for _, row in self.edges.iterrows():
            src = row.get(':START_ID', row.get('source', row.get('from')))
            dst = row.get(':END_ID', row.get('target', row.get('to')))
            weight = row.get('weight', 1.0)
            
            self.out_edges[src].append(dst)
            self.in_edges[dst].append(src)
            self.edge_weights[(src, dst)] = weight
    
    def compute_pagerank(self, iterations=20, damping=0.85):
        """Compute PageRank scores"""
        print("\n🔍 Computing PageRank...")
        
        # Get all nodes from the nodes file
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        nodes_from_file = set(self.nodes[node_col])
        
        # Get all nodes that appear in edges (some might not be in nodes.csv)
        nodes_from_edges = set()
        for node in self.out_edges.keys():
            nodes_from_edges.add(node)
        for node in self.in_edges.keys():
            nodes_from_edges.add(node)
        
        # Use union of both (all nodes that exist anywhere)
        all_nodes = nodes_from_file | nodes_from_edges
        nodes = list(all_nodes)
        n = len(nodes)
        
        print(f"  ℹ️  Nodes in nodes.csv: {len(nodes_from_file)}")
        print(f"  ℹ️  Nodes in edges.csv: {len(nodes_from_edges)}")
        print(f"  ℹ️  Total unique nodes: {n}")
        
        # Initialize PageRank
        pr = {node: 1.0 / n for node in nodes}
        
        for iteration in range(iterations):
            new_pr = {}
            
            for node in nodes:
                rank_sum = 0.0
                
                # Sum contributions from incoming edges
                for in_node in self.in_edges.get(node, []):
                    out_degree = len(self.out_edges.get(in_node, []))
                    if out_degree > 0 and in_node in pr:
                        rank_sum += pr[in_node] / out_degree
                
                new_pr[node] = (1 - damping) / n + damping * rank_sum
            
            pr = new_pr
        
        print(f"  ✓ Computed PageRank for {len(pr)} nodes")
        return pr
    
    def compute_degree_centrality(self):
        """Compute in-degree, out-degree, total degree"""
        print("\n📊 Computing degree centrality...")
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        
        # Get all nodes (from file + edges)
        nodes_from_file = set(self.nodes[node_col])
        nodes_from_edges = set(list(self.out_edges.keys()) + list(self.in_edges.keys()))
        all_nodes = list(nodes_from_file | nodes_from_edges)
        
        centrality = {}
        for node in all_nodes:
            in_deg = len(self.in_edges.get(node, []))
            out_deg = len(self.out_edges.get(node, []))
            
            centrality[node] = {
                'in_degree': in_deg,
                'out_degree': out_deg,
                'total_degree': in_deg + out_deg
            }
        
        print(f"  ✓ Computed degree centrality for {len(centrality)} nodes")
        return centrality
    
    def compute_betweenness_centrality(self, sample_size=100):
        """Compute betweenness centrality (sampled for efficiency)"""
        print("\n🔗 Computing betweenness centrality...")
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        
        # Get all nodes
        nodes_from_file = set(self.nodes[node_col])
        nodes_from_edges = set(list(self.out_edges.keys()) + list(self.in_edges.keys()))
        all_nodes = list(nodes_from_file | nodes_from_edges)
        
        betweenness = {node: 0.0 for node in all_nodes}
        
        # Sample nodes for BFS
        sample_nodes = np.random.choice(all_nodes, min(sample_size, len(all_nodes)), replace=False)
        
        for source in sample_nodes:
            # BFS to find shortest paths
            shortest_paths = self._bfs_shortest_paths(source, all_nodes)
            
            # Update betweenness scores
            for target in all_nodes:
                if target != source and target in shortest_paths:
                    for node in shortest_paths[target]:
                        if node != source and node != target:
                            betweenness[node] += 1
        
        # Normalize
        n = len(all_nodes)
        if n > 2:
            norm = (n - 1) * (n - 2)
            betweenness = {k: v / norm for k, v in betweenness.items()}
        
        print(f"  ✓ Computed betweenness for {len(betweenness)} nodes")
        return betweenness
    
    def _bfs_shortest_paths(self, source, all_nodes):
        """BFS to find shortest paths from source"""
        queue = deque([(source, [source])])
        visited = {source}
        paths = {}
        
        while queue:
            node, path = queue.popleft()
            
            for neighbor in self.out_edges[node]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    new_path = path + [neighbor]
                    paths[neighbor] = new_path
                    queue.append((neighbor, new_path))
        
        return paths
    
    def compute_clustering_coefficient(self):
        """Compute local clustering coefficient"""
        print("\n🔢 Computing clustering coefficients...")
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        
        # Get all nodes
        nodes_from_file = set(self.nodes[node_col])
        nodes_from_edges = set(list(self.out_edges.keys()) + list(self.in_edges.keys()))
        all_nodes = list(nodes_from_file | nodes_from_edges)
        
        clustering = {}
        
        for node in all_nodes:
            neighbors = set(self.out_edges.get(node, [])) | set(self.in_edges.get(node, []))
            k = len(neighbors)
            
            if k < 2:
                clustering[node] = 0.0
                continue
            
            # Count edges between neighbors
            edges_between = 0
            for n1 in neighbors:
                for n2 in neighbors:
                    if n1 != n2 and (n1, n2) in self.edge_weights:
                        edges_between += 1
            
            clustering[node] = edges_between / (k * (k - 1)) if k > 1 else 0.0
        
        print(f"  ✓ Computed clustering for {len(clustering)} nodes")
        return clustering
    
    def compute_exposure_metrics(self):
        """Compute financial exposure metrics"""
        print("\n💰 Computing exposure metrics...")
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        
        # Get all nodes
        nodes_from_file = set(self.nodes[node_col])
        nodes_from_edges = set(list(self.out_edges.keys()) + list(self.in_edges.keys()))
        all_nodes = list(nodes_from_file | nodes_from_edges)
        
        exposure = {}
        
        for node in all_nodes:
            # Total incoming exposure
            incoming_exposure = sum(
                self.edge_weights.get((src, node), 0)
                for src in self.in_edges.get(node, [])
            )
            
            # Total outgoing exposure
            outgoing_exposure = sum(
                self.edge_weights.get((node, dst), 0)
                for dst in self.out_edges.get(node, [])
            )
            
            exposure[node] = {
                'incoming_exposure': incoming_exposure,
                'outgoing_exposure': outgoing_exposure,
                'net_exposure': incoming_exposure - outgoing_exposure,
                'total_exposure': incoming_exposure + outgoing_exposure
            }
        
        print(f"  ✓ Computed exposure for {len(exposure)} nodes")
        return exposure
    
    
    def extract_all_features(self):
        """Extract complete feature set"""
        print("\n" + "="*60)
        print("🚀 EXTRACTING ALL NETWORK FEATURES")
        print("="*60)
        
        node_col = 'node_id:ID' if 'node_id:ID' in self.nodes.columns else \
                   'node_id' if 'node_id' in self.nodes.columns else self.nodes.columns[0]
        
        # Compute all metrics
        pagerank = self.compute_pagerank()
        centrality = self.compute_degree_centrality()
        betweenness = self.compute_betweenness_centrality()
        clustering = self.compute_clustering_coefficient()
        exposure = self.compute_exposure_metrics()
        
        # Combine into DataFrame
        features_df = self.nodes.copy()
        
        features_df['pagerank_score'] = features_df[node_col].map(pagerank)
        features_df['in_degree'] = features_df[node_col].map(lambda x: centrality[x]['in_degree'])
        features_df['out_degree'] = features_df[node_col].map(lambda x: centrality[x]['out_degree'])
        features_df['total_degree'] = features_df[node_col].map(lambda x: centrality[x]['total_degree'])
        features_df['betweenness_centrality'] = features_df[node_col].map(betweenness)
        features_df['clustering_coefficient'] = features_df[node_col].map(clustering)
        features_df['incoming_exposure'] = features_df[node_col].map(lambda x: exposure[x]['incoming_exposure'])
        features_df['outgoing_exposure'] = features_df[node_col].map(lambda x: exposure[x]['outgoing_exposure'])
        features_df['net_exposure'] = features_df[node_col].map(lambda x: exposure[x]['net_exposure'])
        features_df['total_exposure'] = features_df[node_col].map(lambda x: exposure[x]['total_exposure'])
        
        # Fill missing values
        features_df = features_df.fillna(0)
        
        print("\n✅ Feature extraction complete!")
        print(f"   Features shape: {features_df.shape}")
        print(f"   Feature columns added: 10")
        
        return features_df
    
    def save_features(self, features_df, output_path):
        """Save features to CSV"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        features_df.to_csv(output_path, index=False)
        print(f"\n💾 Saved features to: {output_path}")

def main():
    extractor = NetworkFeatureExtractor(
        nodes_path="data/nodes.csv",
        edges_path="data/edges.csv"
    )
    
    features = extractor.extract_all_features()
    
    # Display sample
    print("\n📋 Sample features (first 5 rows):")
    feature_cols = ['pagerank_score', 'total_degree', 'betweenness_centrality', 
                    'clustering_coefficient', 'total_exposure']
    available_cols = [col for col in feature_cols if col in features.columns]
    print(features[available_cols].head())
    
    # Save
    extractor.save_features(features, "output/features/network_features.csv")
    
    return features

if __name__ == "__main__":
    main()
