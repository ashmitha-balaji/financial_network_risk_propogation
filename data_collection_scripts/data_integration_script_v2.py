"""
COMPLETE DATA INTEGRATION FOR UPSCALED COLLECTORS - VERSION 2
Merges all collected datasets into unified nodes and edges WITH CROSS-LAYER BRIDGES
Target: 113,600+ rows → Unified multi-layer network for Neo4j with risk propagation paths
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import logging
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UpscaledDataIntegratorV2:
    """
    Integrates all upscaled datasets into unified multi-layer network
    VERSION 2: Includes cross-layer bridge connections for risk propagation
    """
    
    def __init__(self, data_root='data'):
        self.data_root = Path(data_root)
        self.output_dir = self.data_root / 'integrated'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.datasets = {}
        self.entity_mapping = None
        
    def load_all_upscaled_datasets(self):
        """
        Step 1: Load all upscaled datasets
        Time: 30 minutes
        """
        logger.info("📂 Loading all upscaled datasets...")
        
        # 1. BANKING UPSCALED
        banking_upscaled = self.data_root / 'banking_upscaled'
        if banking_upscaled.exists():
            logger.info("\n🏦 Loading Banking Upscaled...")
            
            # Banking indicators timeseries
            files = list(banking_upscaled.glob('banking_indicators_timeseries_*.csv'))
            if files:
                self.datasets['banking_indicators'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Banking indicators: {len(self.datasets['banking_indicators']):,} rows")
            
            # Interbank lending network
            files = list(banking_upscaled.glob('interbank_lending_network_*.csv'))
            if files:
                self.datasets['interbank_lending'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Interbank lending: {len(self.datasets['interbank_lending']):,} rows")
            
            # Institution profiles
            files = list(banking_upscaled.glob('institution_profiles_*.csv'))
            if files:
                self.datasets['institution_profiles'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Institution profiles: {len(self.datasets['institution_profiles']):,} rows")
            
            # Stress events
            files = list(banking_upscaled.glob('stress_events_*.csv'))
            if files:
                self.datasets['stress_events'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Stress events: {len(self.datasets['stress_events']):,} rows")
            
            # Daily positions
            files = list(banking_upscaled.glob('daily_positions_*.csv'))
            if files:
                self.datasets['daily_positions'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Daily positions: {len(self.datasets['daily_positions']):,} rows")
        
        # 2. MARKET UPSCALED
        market_upscaled = self.data_root / 'market_upscaled'
        if market_upscaled.exists():
            logger.info("\n📈 Loading Market Upscaled...")
            
            # Prices with indicators
            files = list(market_upscaled.glob('prices_with_indicators_*.csv'))
            if files:
                self.datasets['market_prices'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Market prices: {len(self.datasets['market_prices']):,} rows")
            
            # Market correlations
            files = list(market_upscaled.glob('market_correlations_*.csv'))
            if files:
                self.datasets['market_correlations'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Market correlations: {len(self.datasets['market_correlations']):,} rows")
            
            # Company fundamentals
            files = list(market_upscaled.glob('company_fundamentals_*.csv'))
            if files:
                self.datasets['company_fundamentals'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Company fundamentals: {len(self.datasets['company_fundamentals']):,} rows")
            
            # Market events
            files = list(market_upscaled.glob('market_events_*.csv'))
            if files:
                self.datasets['market_events'] = pd.read_csv(max(files, key=lambda p: p.stat().st_mtime))
                logger.info(f"  ✓ Market events: {len(self.datasets['market_events']):,} rows")
        
        # 3. FDIC BANKING (if separate from upscaled)
        banking_dir = self.data_root / 'banking'
        if banking_dir.exists():
            logger.info("\n🏦 Loading FDIC Banking...")
            
            # Active banks
            active_banks_file = banking_dir / 'active_banks.csv'
            if active_banks_file.exists():
                self.datasets['fdic_active_banks'] = pd.read_csv(active_banks_file)
                logger.info(f"  ✓ FDIC active banks: {len(self.datasets['fdic_active_banks']):,} rows")
            
            # Failed banks
            failed_banks_file = banking_dir / 'failed_banks.csv'
            if failed_banks_file.exists():
                self.datasets['fdic_failed_banks'] = pd.read_csv(failed_banks_file)
                logger.info(f"  ✓ FDIC failed banks: {len(self.datasets['fdic_failed_banks']):,} rows")
        
        # 4. OWNERSHIP
        ownership_dir = self.data_root / 'ownership'
        if ownership_dir.exists():
            logger.info("\n🏢 Loading Ownership...")
            
            # Company CIK mapping
            cik_file = ownership_dir / 'company_cik_mapping.csv'
            if cik_file.exists():
                self.datasets['company_cik'] = pd.read_csv(cik_file)
                logger.info(f"  ✓ Company CIK: {len(self.datasets['company_cik']):,} rows")
            
            # 13F holdings
            holdings_file = ownership_dir / '13f_institutional_holdings.csv'
            if holdings_file.exists():
                self.datasets['13f_holdings'] = pd.read_csv(holdings_file)
                logger.info(f"  ✓ 13F holdings: {len(self.datasets['13f_holdings']):,} rows")
            
            # Ownership network
            ownership_file = ownership_dir / 'corporate_ownership_network.csv'
            if ownership_file.exists():
                self.datasets['ownership_network'] = pd.read_csv(ownership_file)
                logger.info(f"  ✓ Ownership network: {len(self.datasets['ownership_network']):,} rows")
        
        # 5. ECONOMIC
        logger.info("\n📉 Loading Economic...")
        econ_files = list(self.data_root.glob('**/fred_economic_*.csv'))
        if econ_files:
            self.datasets['economic_indicators'] = pd.read_csv(max(econ_files, key=lambda p: p.stat().st_mtime))
            logger.info(f"  ✓ Economic indicators: {len(self.datasets['economic_indicators']):,} rows")
        
        total_rows = sum(len(df) for df in self.datasets.values())
        logger.info(f"\n✅ Loaded {len(self.datasets)} datasets with {total_rows:,} total rows")
        
        return self.datasets
    
    def create_unified_nodes(self):
        """
        Step 2: Create unified node list
        Time: 1-2 hours
        """
        logger.info("\n📊 Creating unified node list...")
        
        nodes = []
        node_id_set = set()  # Track unique node IDs
        
        # 1. BANKING INSTITUTIONS (from profiles)
        if 'institution_profiles' in self.datasets:
            df = self.datasets['institution_profiles']
            for _, inst in df.iterrows():
                node_id = f"BANK_{inst['institution_id']}"
                if node_id not in node_id_set:
                    nodes.append({
                        'node_id': node_id,
                        'node_type': 'bank',
                        'network_layer': 'banking',
                        'name': inst.get('institution_name', inst['institution_id']),
                        'institution_id': inst['institution_id'],
                        'tier': inst.get('tier', 'unknown'),
                        'total_assets': float(inst.get('total_assets', 0)),
                        'total_deposits': float(inst.get('total_deposits', 0)),
                        'total_loans': float(inst.get('total_loans', 0)),
                        'equity': float(inst.get('equity', 0)),
                        'num_branches': int(inst.get('num_branches', 0)),
                        'num_employees': int(inst.get('num_employees', 0))
                    })
                    node_id_set.add(node_id)
        
        logger.info(f"  ✓ Added {len([n for n in nodes if n['node_type'] == 'bank'])} banking nodes")
        
        # 2. FDIC BANKS (if different from upscaled)
        if 'fdic_active_banks' in self.datasets:
            df = self.datasets['fdic_active_banks']
            for _, bank in df.iterrows():
                cert = bank.get('CERT', bank.get('cert'))
                if pd.notna(cert):
                    node_id = f"FDIC_{int(cert)}"
                    if node_id not in node_id_set:
                        nodes.append({
                            'node_id': node_id,
                            'node_type': 'fdic_bank',
                            'network_layer': 'banking',
                            'name': bank.get('NAME', bank.get('name', 'Unknown')),
                            'cert': int(cert),
                            'assets': float(bank.get('ASSET', bank.get('asset', 0))) if pd.notna(bank.get('ASSET', bank.get('asset'))) else 0,
                            'city': str(bank.get('CITY', bank.get('city', ''))),
                            'state': str(bank.get('STALP', bank.get('stalp', '')))
                        })
                        node_id_set.add(node_id)
        
        # 3. MARKET STOCKS
        if 'company_fundamentals' in self.datasets:
            df = self.datasets['company_fundamentals']
            for _, company in df.iterrows():
                symbol = company.get('symbol', company.get('ticker'))
                if pd.notna(symbol):
                    node_id = f"STOCK_{symbol}"
                    if node_id not in node_id_set:
                        nodes.append({
                            'node_id': node_id,
                            'node_type': 'stock',
                            'network_layer': 'market',
                            'ticker': symbol,
                            'name': company.get('company_name', symbol),
                            'sector': company.get('sector', 'Unknown'),
                            'industry': company.get('industry', 'Unknown'),
                            'market_cap': float(company.get('market_cap', 0)),
                            'trailing_pe': float(company.get('trailing_pe', 0)) if pd.notna(company.get('trailing_pe')) else None,
                            'debt_to_equity': float(company.get('debt_to_equity', 0)) if pd.notna(company.get('debt_to_equity')) else None,
                            'beta': float(company.get('beta', 1.0)) if pd.notna(company.get('beta')) else 1.0
                        })
                        node_id_set.add(node_id)
        
        logger.info(f"  ✓ Added {len([n for n in nodes if n['node_type'] == 'stock'])} stock nodes")
        
        # 4. INSTITUTIONAL INVESTORS
        if '13f_holdings' in self.datasets:
            df = self.datasets['13f_holdings']
            unique_institutions = df['filer_cik'].unique()
            
            for cik in unique_institutions:
                if pd.notna(cik):
                    node_id = f"INST_{cik}"
                    if node_id not in node_id_set:
                        nodes.append({
                            'node_id': node_id,
                            'node_type': 'institutional_investor',
                            'network_layer': 'ownership',
                            'cik': str(cik),
                            'name': f"Institution_{cik}"
                        })
                        node_id_set.add(node_id)
        
        logger.info(f"  ✓ Added {len([n for n in nodes if n['node_type'] == 'institutional_investor'])} institutional nodes")
        
        # Convert to DataFrame
        df_nodes = pd.DataFrame(nodes)
        df_nodes['created_at'] = datetime.now().isoformat()
        
        # Save
        output_file = self.output_dir / 'unified_nodes.csv'
        df_nodes.to_csv(output_file, index=False)
        logger.info(f"\n✅ Created {len(df_nodes):,} unified nodes → {output_file}")
        
        return df_nodes
    
    def create_entity_mapping(self, df_nodes):
        """
        NEW: Create mapping of entities across layers by ticker
        This identifies banks that are also publicly traded stocks
        """
        logger.info("\n🔗 Creating cross-layer entity mapping...")
        
        # Extract banking entities with their tickers
        banking_entities = df_nodes[df_nodes['node_type'] == 'bank'].copy()
        banking_tickers = {}
        for _, bank in banking_entities.iterrows():
            ticker = bank['institution_id']
            banking_tickers[ticker] = {
                'bank_node_id': bank['node_id'],
                'bank_name': bank['name'],
                'total_assets': bank.get('total_assets', 0)
            }
        
        # Extract market entities with their tickers
        market_entities = df_nodes[df_nodes['node_type'] == 'stock'].copy()
        market_tickers = {}
        for _, stock in market_entities.iterrows():
            ticker = stock['ticker']
            market_tickers[ticker] = {
                'stock_node_id': stock['node_id'],
                'stock_name': stock['name'],
                'market_cap': stock.get('market_cap', 0)
            }
        
        # Find matches
        mapping_data = []
        matched_tickers = set(banking_tickers.keys()) & set(market_tickers.keys())
        
        for ticker in matched_tickers:
            mapping_data.append({
                'ticker': ticker,
                'bank_node_id': banking_tickers[ticker]['bank_node_id'],
                'stock_node_id': market_tickers[ticker]['stock_node_id'],
                'entity_name': banking_tickers[ticker]['bank_name'],
                'entity_type': 'publicly_traded_bank',
                'matched': True,
                'total_assets': banking_tickers[ticker]['total_assets'],
                'market_cap': market_tickers[ticker]['market_cap']
            })
        
        # Add unmatched banking entities
        unmatched_banks = set(banking_tickers.keys()) - matched_tickers
        for ticker in unmatched_banks:
            mapping_data.append({
                'ticker': ticker,
                'bank_node_id': banking_tickers[ticker]['bank_node_id'],
                'stock_node_id': None,
                'entity_name': banking_tickers[ticker]['bank_name'],
                'entity_type': 'bank_not_public',
                'matched': False,
                'total_assets': banking_tickers[ticker]['total_assets'],
                'market_cap': 0
            })
        
        # Add unmatched market entities
        unmatched_stocks = set(market_tickers.keys()) - matched_tickers
        for ticker in unmatched_stocks:
            mapping_data.append({
                'ticker': ticker,
                'bank_node_id': None,
                'stock_node_id': market_tickers[ticker]['stock_node_id'],
                'entity_name': market_tickers[ticker]['stock_name'],
                'entity_type': 'stock_not_bank',
                'matched': False,
                'total_assets': 0,
                'market_cap': market_tickers[ticker]['market_cap']
            })
        
        df_mapping = pd.DataFrame(mapping_data)
        
        # Save mapping
        output_file = self.output_dir / 'entity_mapping.csv'
        df_mapping.to_csv(output_file, index=False)
        
        logger.info(f"  ✓ Total entities: {len(df_mapping)}")
        logger.info(f"  ✓ Matched (bank + stock): {len(matched_tickers)}")
        logger.info(f"  ✓ Banks only: {len(unmatched_banks)}")
        logger.info(f"  ✓ Stocks only: {len(unmatched_stocks)}")
        logger.info(f"  ✓ Saved mapping → {output_file}")
        
        # Store for later use
        self.entity_mapping = df_mapping
        
        return df_mapping
    
    def create_cross_layer_bridge_edges(self, entity_mapping):
        """
        NEW: Create bidirectional bridge edges between banking and market layers
        Enables risk propagation: BANK_JPM <--> STOCK_JPM
        """
        logger.info("\n🌉 Creating cross-layer bridge edges...")
        
        bridge_edges = []
        
        # Filter to matched entities only
        matched_entities = entity_mapping[entity_mapping['matched'] == True]
        
        for _, match in matched_entities.iterrows():
            bank_node = match['bank_node_id']
            stock_node = match['stock_node_id']
            ticker = match['ticker']
            
            # Bridge 1: Bank → Stock (PUBLICLY_TRADED_AS)
            bridge_edges.append({
                'source_id': bank_node,
                'target_id': stock_node,
                'relationship_type': 'PUBLICLY_TRADED_AS',
                'network_layer': 'cross_layer_bridge',
                'layer_bridge': True,
                'bridge_type': 'corporate_identity',
                'ticker': ticker,
                'weight': 1.0,
                'propagation_factor': 0.9  # How much stress propagates
            })
            
            # Bridge 2: Stock → Bank (REPRESENTS_BANK)
            bridge_edges.append({
                'source_id': stock_node,
                'target_id': bank_node,
                'relationship_type': 'REPRESENTS_BANK',
                'network_layer': 'cross_layer_bridge',
                'layer_bridge': True,
                'bridge_type': 'corporate_identity',
                'ticker': ticker,
                'weight': 1.0,
                'propagation_factor': 0.8  # Stock crash impacts bank confidence
            })
        
        df_bridges = pd.DataFrame(bridge_edges)
        
        logger.info(f"  ✓ Created {len(df_bridges)} bridge edges ({len(matched_entities)} entities × 2 directions)")
        logger.info(f"  ✓ Relationship types: {df_bridges['relationship_type'].unique().tolist()}")
        
        return df_bridges
    
    def create_unified_edges(self):
        """
        Step 3: Create unified edge list (MODIFIED to include bridge edges)
        Time: 1-2 hours
        """
        logger.info("\n🔗 Creating unified edge list...")
        
        edges = []
        edge_set = set()  # Track unique edges (source, target, type)
        
        # 1. INTERBANK LENDING EDGES
        if 'interbank_lending' in self.datasets:
            df = self.datasets['interbank_lending']
            for _, txn in df.iterrows():
                from_inst = txn.get('from_institution')
                to_inst = txn.get('to_institution')
                
                if pd.notna(from_inst) and pd.notna(to_inst):
                    source_id = f"BANK_{from_inst}"
                    target_id = f"BANK_{to_inst}"
                    rel_type = txn.get('relationship_type', 'interbank_lending')
                    
                    edge_key = (source_id, target_id, rel_type)
                    if edge_key not in edge_set:
                        edges.append({
                            'source_id': source_id,
                            'target_id': target_id,
                            'relationship_type': rel_type,
                            'network_layer': 'banking',
                            'weight': float(txn.get('amount_usd', 0)),
                            'transaction_date': txn.get('transaction_date'),
                            'maturity_days': int(txn.get('maturity_days', 0)),
                            'interest_rate': float(txn.get('interest_rate', 0)),
                            'currency': 'USD'
                        })
                        edge_set.add(edge_key)
        
        logger.info(f"  ✓ Added {len([e for e in edges if e['network_layer'] == 'banking'])} banking edges")
        
        # 2. MARKET CORRELATION EDGES
        if 'market_correlations' in self.datasets:
            df = self.datasets['market_correlations']
            for _, corr in df.iterrows():
                symbol1 = corr.get('symbol1')
                symbol2 = corr.get('symbol2')
                
                if pd.notna(symbol1) and pd.notna(symbol2):
                    source_id = f"STOCK_{symbol1}"
                    target_id = f"STOCK_{symbol2}"
                    rel_type = 'market_correlation'
                    
                    edge_key = (source_id, target_id, rel_type)
                    if edge_key not in edge_set:
                        edges.append({
                            'source_id': source_id,
                            'target_id': target_id,
                            'relationship_type': rel_type,
                            'network_layer': 'market',
                            'weight': abs(float(corr.get('correlation', 0))),
                            'correlation': float(corr.get('correlation', 0)),
                            'window_days': int(corr.get('window_days', 90))
                        })
                        edge_set.add(edge_key)
        
        logger.info(f"  ✓ Added {len([e for e in edges if e['network_layer'] == 'market'])} market edges")
        
        # 3. OWNERSHIP EDGES
        if 'ownership_network' in self.datasets:
            df = self.datasets['ownership_network']
            for _, own in df.iterrows():
                owner_cik = own.get('owner_cik')
                owned_ticker = own.get('owned_ticker')
                
                if pd.notna(owner_cik) and pd.notna(owned_ticker):
                    source_id = f"INST_{owner_cik}"
                    target_id = f"STOCK_{owned_ticker}"
                    rel_type = own.get('relationship_type', 'equity_ownership')
                    
                    edge_key = (source_id, target_id, rel_type)
                    if edge_key not in edge_set:
                        edges.append({
                            'source_id': source_id,
                            'target_id': target_id,
                            'relationship_type': rel_type,
                            'network_layer': 'ownership',
                            'weight': float(own.get('ownership_value', 0)),
                            'shares_held': int(own.get('shares_held', 0)) if pd.notna(own.get('shares_held')) else 0
                        })
                        edge_set.add(edge_key)
        
        logger.info(f"  ✓ Added {len([e for e in edges if e['network_layer'] == 'ownership'])} ownership edges")
        
        # 4. CROSS-LAYER BRIDGE EDGES (NEW!)
        if self.entity_mapping is not None:
            df_bridges = self.create_cross_layer_bridge_edges(self.entity_mapping)
            
            # Add bridge edges to main edge list
            for _, bridge in df_bridges.iterrows():
                edge_key = (bridge['source_id'], bridge['target_id'], bridge['relationship_type'])
                if edge_key not in edge_set:
                    edges.append(bridge.to_dict())
                    edge_set.add(edge_key)
            
            logger.info(f"  ✓ Added {len(df_bridges)} cross-layer bridge edges")
        
        # Convert to DataFrame
        df_edges = pd.DataFrame(edges)
        df_edges['created_at'] = datetime.now().isoformat()
        
        # Save
        output_file = self.output_dir / 'unified_edges.csv'
        df_edges.to_csv(output_file, index=False)
        logger.info(f"\n✅ Created {len(df_edges):,} unified edges → {output_file}")
        
        return df_edges
    
    def create_network_statistics(self, df_nodes, df_edges):
        """
        Step 4: Generate network statistics (MODIFIED to include cross-layer metrics)
        Time: 30 minutes
        """
        logger.info("\n📈 Generating network statistics...")
        
        stats = {
            'timestamp': datetime.now().isoformat(),
            'total_nodes': len(df_nodes),
            'total_edges': len(df_edges),
            'layers': {},
            'datasets_used': list(self.datasets.keys())
        }
        
        # Statistics by layer
        for layer in df_nodes['network_layer'].unique():
            layer_nodes = df_nodes[df_nodes['network_layer'] == layer]
            layer_edges = df_edges[df_edges['network_layer'] == layer]
            
            stats['layers'][layer] = {
                'nodes': len(layer_nodes),
                'edges': len(layer_edges),
                'node_types': layer_nodes['node_type'].value_counts().to_dict(),
                'relationship_types': layer_edges['relationship_type'].value_counts().to_dict() if len(layer_edges) > 0 else {},
                'avg_degree': float(len(layer_edges) / len(layer_nodes)) if len(layer_nodes) > 0 else 0,
                'density': float(len(layer_edges) / (len(layer_nodes) * (len(layer_nodes) - 1))) if len(layer_nodes) > 1 else 0
            }
        
        # NEW: Cross-layer connection statistics
        bridge_edges = df_edges[df_edges['network_layer'] == 'cross_layer_bridge']
        
        if len(bridge_edges) > 0:
            matched_entities = self.entity_mapping[self.entity_mapping['matched'] == True] if self.entity_mapping is not None else pd.DataFrame()
            
            stats['cross_layer_connections'] = {
                'total_bridge_edges': len(bridge_edges),
                'bridge_relationship_types': bridge_edges['relationship_type'].value_counts().to_dict(),
                'connected_entities': len(matched_entities),
                'connected_entity_list': matched_entities['ticker'].tolist() if len(matched_entities) > 0 else []
            }
            
            # Layer connectivity matrix
            stats['layer_connectivity_matrix'] = {
                'banking_to_market': len(bridge_edges[bridge_edges['relationship_type'] == 'PUBLICLY_TRADED_AS']),
                'market_to_banking': len(bridge_edges[bridge_edges['relationship_type'] == 'REPRESENTS_BANK']),
                'market_to_ownership': len(df_edges[
                    (df_edges['network_layer'] == 'ownership') & 
                    (df_edges['source_id'].str.startswith('INST_'))
                ]),
                'banking_to_ownership': 0  # Could be added in future versions
            }
        
        # Save statistics
        stats_file = self.output_dir / 'network_statistics.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        logger.info(f"✅ Saved network statistics → {stats_file}")
        
        return stats
    
    def generate_validation_report(self, entity_mapping, df_nodes, df_edges):
        """
        NEW: Generate detailed validation report for cross-layer connections
        """
        logger.info("\n📋 Generating cross-layer validation report...")
        
        report_lines = []
        report_lines.append("="*80)
        report_lines.append("CROSS-LAYER CONNECTION VALIDATION REPORT")
        report_lines.append("="*80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Node counts by layer
        report_lines.append("NODE COUNTS BY LAYER")
        report_lines.append("-" * 40)
        for layer in df_nodes['network_layer'].unique():
            count = len(df_nodes[df_nodes['network_layer'] == layer])
            report_lines.append(f"  {layer.capitalize()}: {count} nodes")
        report_lines.append("")
        
        # Entity matching results
        matched = entity_mapping[entity_mapping['matched'] == True]
        banks_only = entity_mapping[entity_mapping['entity_type'] == 'bank_not_public']
        stocks_only = entity_mapping[entity_mapping['entity_type'] == 'stock_not_bank']
        
        report_lines.append("ENTITY MATCHING RESULTS")
        report_lines.append("-" * 40)
        report_lines.append(f"  Matched (Bank + Stock): {len(matched)} entities")
        report_lines.append(f"  Banks only (not public): {len(banks_only)} entities")
        report_lines.append(f"  Stocks only (not banks): {len(stocks_only)} entities")
        report_lines.append(f"  Total entities: {len(entity_mapping)} entities")
        report_lines.append("")
        
        # Matched entities list
        if len(matched) > 0:
            report_lines.append("MATCHED ENTITIES (Bank ↔ Stock)")
            report_lines.append("-" * 40)
            for _, entity in matched.iterrows():
                report_lines.append(f"  {entity['ticker']}: {entity['entity_name']}")
            report_lines.append("")
        
        # Bridge edges
        bridge_edges = df_edges[df_edges['network_layer'] == 'cross_layer_bridge']
        report_lines.append("BRIDGE EDGES CREATED")
        report_lines.append("-" * 40)
        report_lines.append(f"  Total bridge edges: {len(bridge_edges)}")
        for rel_type, count in bridge_edges['relationship_type'].value_counts().items():
            report_lines.append(f"    {rel_type}: {count} edges")
        report_lines.append("")
        
        # Risk propagation paths
        report_lines.append("RISK PROPAGATION PATHS")
        report_lines.append("-" * 40)
        report_lines.append("  Banking → Market:")
        report_lines.append(f"    Via PUBLICLY_TRADED_AS: {len(matched)} paths")
        report_lines.append("  Market → Ownership:")
        ownership_edges = df_edges[df_edges['network_layer'] == 'ownership']
        report_lines.append(f"    Via equity_ownership: {len(ownership_edges)} paths")
        report_lines.append("  Full chain (Banking → Market → Ownership):")
        report_lines.append(f"    Possible paths: {len(matched) * len(ownership_edges)} combinations")
        report_lines.append("")
        
        # Example contagion scenario
        if len(matched) > 0:
            example = matched.iloc[0]
            report_lines.append("EXAMPLE CONTAGION SCENARIO")
            report_lines.append("-" * 40)
            report_lines.append(f"  Trigger: {example['bank_node_id']} experiences stress event")
            report_lines.append(f"  Step 1: Propagates via PUBLICLY_TRADED_AS → {example['stock_node_id']}")
            report_lines.append(f"  Step 2: Stock price drops, affecting institutional investors")
            report_lines.append(f"  Step 3: Investors (INST_*) holding {example['ticker']} face losses")
            report_lines.append(f"  Step 4: Forced selling cascades to correlated stocks")
            report_lines.append("")
        
        report_lines.append("="*80)
        report_lines.append("VALIDATION COMPLETE")
        report_lines.append("="*80)
        
        # Save report
        report_file = self.output_dir / 'cross_layer_validation_report.txt'
        with open(report_file, 'w') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"✅ Saved validation report → {report_file}")
        
        # Also print to console
        print("\n" + '\n'.join(report_lines))
        
        return report_file
    
    def generate_neo4j_import_files(self, df_nodes, df_edges):
        """
        Step 5: Generate Neo4j-ready import files
        Time: 30 minutes
        """
        logger.info("\n🔧 Generating Neo4j import files...")
        
        neo4j_dir = self.output_dir / 'neo4j_import'
        neo4j_dir.mkdir(exist_ok=True)
        
        # Nodes - Neo4j format
        df_nodes_neo4j = df_nodes.copy()
        df_nodes_neo4j.rename(columns={'node_id': 'node_id:ID'}, inplace=True)
        df_nodes_neo4j[':LABEL'] = df_nodes_neo4j['node_type']
        
        nodes_file = neo4j_dir / 'nodes.csv'
        df_nodes_neo4j.to_csv(nodes_file, index=False)
        logger.info(f"  ✓ Nodes → {nodes_file}")
        
        # Edges - Neo4j format
        df_edges_neo4j = df_edges.copy()
        df_edges_neo4j.rename(columns={
            'source_id': ':START_ID',
            'target_id': ':END_ID',
            'relationship_type': ':TYPE'
        }, inplace=True)
        
        edges_file = neo4j_dir / 'edges.csv'
        df_edges_neo4j.to_csv(edges_file, index=False)
        logger.info(f"  ✓ Edges → {edges_file}")
        
        logger.info(f"\n✅ Neo4j import files ready in: {neo4j_dir}")
        
        return neo4j_dir
    
    def print_summary(self, stats):
        """Print final summary (MODIFIED to include cross-layer info)"""
        print("\n" + "="*80)
        print("DATA INTEGRATION COMPLETE ✅ (VERSION 2 - WITH CROSS-LAYER BRIDGES)")
        print("="*80)
        print(f"\n📊 Network Summary:")
        print(f"  Total Nodes: {stats['total_nodes']:,}")
        print(f"  Total Edges: {stats['total_edges']:,}")
        print(f"  Datasets Used: {len(stats['datasets_used'])}")
        
        print(f"\n🔍 By Layer:")
        for layer, layer_stats in stats['layers'].items():
            print(f"\n  {layer.upper()} Layer:")
            print(f"    Nodes: {layer_stats['nodes']:,}")
            print(f"    Edges: {layer_stats['edges']:,}")
            print(f"    Avg Degree: {layer_stats['avg_degree']:.2f}")
            print(f"    Density: {layer_stats['density']:.4f}")
        
        # NEW: Cross-layer summary
        if 'cross_layer_connections' in stats:
            print(f"\n🌉 Cross-Layer Bridges:")
            print(f"    Bridge Edges: {stats['cross_layer_connections']['total_bridge_edges']}")
            print(f"    Connected Entities: {stats['cross_layer_connections']['connected_entities']}")
            print(f"    Banking ↔ Market: {stats['layer_connectivity_matrix']['banking_to_market']} paths")
            print(f"    Market → Ownership: {stats['layer_connectivity_matrix']['market_to_ownership']} paths")
        
        print("\n" + "="*80)
        print(f"📁 Output files in: data/integrated/")
        print("="*80)


def main():
    """
    Execute Complete Data Integration - Version 2
    """
    print("="*80)
    print("COMPLETE DATA INTEGRATION FOR UPSCALED COLLECTORS - V2")
    print("WITH CROSS-LAYER BRIDGE CONNECTIONS")
    print("="*80)
    
    integrator = UpscaledDataIntegratorV2(data_root='data')
    
    # Step 1: Load all datasets
    print("\n[Step 1/7] Loading all upscaled datasets...")
    datasets = integrator.load_all_upscaled_datasets()
    
    if not datasets:
        print("❌ ERROR: No datasets found!")
        print("   Please ensure you've run all data collectors first.")
        return
    
    # Step 2: Create unified nodes
    print("\n[Step 2/7] Creating unified node list...")
    df_nodes = integrator.create_unified_nodes()
    
    # Step 3: Create entity mapping (NEW!)
    print("\n[Step 3/7] Creating cross-layer entity mapping...")
    entity_mapping = integrator.create_entity_mapping(df_nodes)
    
    # Step 4: Create unified edges (with bridges!)
    print("\n[Step 4/7] Creating unified edge list with cross-layer bridges...")
    df_edges = integrator.create_unified_edges()
    
    # Step 5: Generate statistics
    print("\n[Step 5/7] Generating network statistics...")
    stats = integrator.create_network_statistics(df_nodes, df_edges)
    
    # Step 6: Generate validation report (NEW!)
    print("\n[Step 6/7] Generating validation report...")
    report_file = integrator.generate_validation_report(entity_mapping, df_nodes, df_edges)
    
    # Step 7: Create Neo4j import files
    print("\n[Step 7/7] Generating Neo4j import files...")
    neo4j_dir = integrator.generate_neo4j_import_files(df_nodes, df_edges)
    
    # Print summary
    integrator.print_summary(stats)
    
    print("\n✅ Ready for Neo4j import with cross-layer risk propagation!")
    print(f"   Import files: {neo4j_dir}")
    print(f"   Validation report: {report_file}")
    print("\n📋 Next Steps:")
    print("   1. Review validation report for cross-layer connections")
    print("   2. Install Neo4j (Desktop or Docker)")
    print("   3. Import nodes and edges into Neo4j")
    print("   4. Test risk propagation queries:")
    print("      MATCH (b:bank)-[:PUBLICLY_TRADED_AS]->(s:stock)")
    print("      RETURN b.institution_id, s.ticker LIMIT 10")


if __name__ == "__main__":
    main()

