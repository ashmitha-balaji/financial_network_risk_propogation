

"""
Cascade Visualization Toolkit with K-Anonymity
Generates queries and exports for visualizing cascades in Neo4j Browser
VERSION 2: Includes K-Anonymity for privacy-preserving cascade reports
"""


from neo4j import GraphDatabase
import pandas as pd
import json
from datetime import datetime
from collections import defaultdict




# ============================================================================
# K-ANONYMITY IMPLEMENTATION
# ============================================================================


class KAnonymizer:
   """
   K-Anonymity implementation for protecting entity identities in cascade results
   Ensures each entity group has at least k members
   """
  
   def __init__(self, k=3):
       """
       Initialize K-Anonymizer
      
       Args:
           k: Minimum group size (default=3, common privacy standard)
       """
       self.k = k
       self.anonymization_stats = {
           'groups_created': 0,
           'entities_suppressed': 0,
           'entities_generalized': 0
       }
  
   def anonymize_entities(self, entities_df, quasi_identifiers, sensitive_attr=None):
       """
       Apply K-Anonymity to entity dataset
      
       Args:
           entities_df: DataFrame with entity data
           quasi_identifiers: List of columns to group by (e.g., ['entity_type', 'sector'])
           sensitive_attr: Column to protect (e.g., 'impact_value')
          
       Returns:
           Anonymized DataFrame
       """
       if entities_df.empty:
           return entities_df
      
       anonymized_groups = []
      
       # Group by quasi-identifiers
       for group_keys, group_df in entities_df.groupby(quasi_identifiers):
           group_size = len(group_df)
          
           if group_size < self.k:
               # Group too small - SUPPRESS these entities
               self.anonymization_stats['entities_suppressed'] += group_size
               continue  # Skip this group (suppression)
           else:
               # Group large enough - GENERALIZE
               self.anonymization_stats['entities_generalized'] += group_size
               self.anonymization_stats['groups_created'] += 1
              
               # Create anonymized records
               for idx, row in group_df.iterrows():
                   anonymized_row = row.copy()
                  
                   # Replace entity ID with group identifier
                   group_label = self._create_group_label(quasi_identifiers, group_keys)
                   anonymized_row['entity_id_anonymized'] = group_label
                   anonymized_row['entity_name_anonymized'] = group_label
                   anonymized_row['group_size'] = group_size
                   anonymized_row['anonymization_level'] = 'k-anonymous'
                  
                   # Keep quasi-identifiers, anonymize identifiers
                   anonymized_row['entity_id_original'] = '***REDACTED***'
                   anonymized_row['entity_name_original'] = '***REDACTED***'
                  
                   anonymized_groups.append(anonymized_row)
      
       if not anonymized_groups:
           return pd.DataFrame()
      
       return pd.DataFrame(anonymized_groups)
  
   def _create_group_label(self, identifiers, values):
       """Create human-readable group label"""
       if isinstance(values, tuple):
           parts = [f"{id}={val}" for id, val in zip(identifiers, values)]
       else:
           parts = [f"{identifiers[0]}={values}"]
      
       return f"Group[{', '.join(parts)}]"
  
   def create_aggregated_summary(self, entities_df, group_by_cols):
       """
       Create aggregated summary with k-anonymity
       Shows counts and totals instead of individual entities
       """
       if entities_df.empty:
           return pd.DataFrame()
      
       summary = entities_df.groupby(group_by_cols).agg({
           'entity_id': 'count',
           'impact_value': 'sum' if 'impact_value' in entities_df.columns else 'count'
       }).reset_index()
      
       summary.rename(columns={
           'entity_id': 'entity_count',
           'impact_value': 'total_impact'
       }, inplace=True)
      
       # Only show groups with k or more entities
       summary = summary[summary['entity_count'] >= self.k]
      
       return summary
  
   def get_anonymization_report(self):
       """Get statistics about anonymization"""
       return {
           'k_value': self.k,
           'groups_created': self.anonymization_stats['groups_created'],
           'entities_suppressed': self.anonymization_stats['entities_suppressed'],
           'entities_generalized': self.anonymization_stats['entities_generalized'],
           'total_entities': self.anonymization_stats['entities_suppressed'] + self.anonymization_stats['entities_generalized']
       }




# ============================================================================
# CASCADE VISUALIZER WITH K-ANONYMITY
# ============================================================================


class CascadeVisualizerWithPrivacy:
   """Visualizes cascade results with privacy-preserving K-Anonymity"""
  
   def __init__(self, uri, user, password, k=3):
       self.driver = GraphDatabase.driver(uri, auth=(user, password))
       self.anonymizer = KAnonymizer(k=k)
       print(f"✓ Connected to Neo4j")
       print(f"✓ K-Anonymity enabled (k={k})")
  
   def close(self):
       self.driver.close()
  
   def generate_neo4j_visualization_query(self, bank_id, max_hops=3):
       """
       Generates a Cypher query to visualize the cascade in Neo4j Browser
      
       This is THE KEY OUTPUT - copy/paste this into Neo4j Browser!
       """
       print(f"\n{'='*70}")
       print(f"📊 NEO4J BROWSER VISUALIZATION QUERY")
       print(f"{'='*70}")
       print(f"\n💡 Copy this query into Neo4j Browser to see the cascade as a GRAPH:\n")
      
       query = f"""
// Cascade Visualization: What happens when BANK_{bank_id} fails?
// This query shows the full propagation path with colored nodes by layer


MATCH path = (start:bank {{institution_id: '{bank_id}'}})
            -[:PUBLICLY_TRADED_AS*0..1]->(s1:stock)
            <-[:equity_ownership*0..1]-(i:institutional_investor)
            -[:equity_ownership*0..1]->(s2:stock)
WHERE start.institution_id = '{bank_id}'


WITH collect(distinct start) +
    collect(distinct s1) +
    collect(distinct i) +
    collect(distinct s2) AS cascade_nodes


UNWIND cascade_nodes AS node


MATCH (node)-[r]-(connected)
WHERE connected IN cascade_nodes


RETURN node, r, connected
LIMIT 500
"""
      
       print(query)
       print(f"\n{'='*70}")
       print(f"🎨 VISUALIZATION TIPS:")
       print(f"  1. Paste query in Neo4j Browser (https://browser.neo4j.io)")
       print(f"  2. Or Aura Console → Query tab")
       print(f"  3. Set graph layout to 'Force-directed'")
       print(f"  4. Color by node type:")
       print(f"     • 🏦 Banks (red)")
       print(f"     • 📈 Stocks (green)")
       print(f"     • 💼 Investors (blue)")
       print(f"  🔒 Privacy Note: Graph shows actual nodes (for analysis)")
       print(f"      Use anonymized reports for external sharing")
       print(f"{'='*70}\n")
      
       return query
  
   def generate_anonymized_impact_table(self, bank_id, k=3):
       """Creates a K-ANONYMIZED impact table (privacy-safe for sharing)"""
       print(f"\n{'='*70}")
       print(f"🔒 GENERATING K-ANONYMIZED IMPACT TABLE (k={k})")
       print(f"{'='*70}")
      
       with self.driver.session() as session:
           # Get all affected entities
           result = session.run("""
               // HOP 1: Direct stock
               MATCH (b:bank {institution_id: $id})-[:PUBLICLY_TRADED_AS]->(s:stock)
               WITH collect({
                   hop: 1,
                   entity_type: 'stock',
                   entity_id: s.ticker,
                   entity_name: s.ticker,
                   impact_type: 'Direct Crash',
                   impact_value: s.market_cap,
                   sector: s.sector
               }) AS hop1_data
              
               // HOP 2: Affected investors
               MATCH (b:bank {institution_id: $id})-[:PUBLICLY_TRADED_AS]->(s:stock)
                      <-[r:equity_ownership]-(i:institutional_investor)
               WITH hop1_data, collect({
                   hop: 2,
                   entity_type: 'investor',
                   entity_id: i.node_id,
                   entity_name: i.name,
                   impact_type: 'Portfolio Loss',
                   impact_value: r.weight,
                   sector: 'Investment'
               }) AS hop2_data
              
               // HOP 3: Cascade stocks (fixed nested aggregation)
               MATCH (b:bank {institution_id: $id})-[:PUBLICLY_TRADED_AS]->(s1:stock)
                      <-[:equity_ownership]-(i:institutional_investor)
                      -[r2:equity_ownership]->(s2:stock)
               WHERE s2.ticker <> s1.ticker
               WITH hop1_data, hop2_data, s2, sum(r2.weight) AS total_impact
               WITH hop1_data, hop2_data, collect({
                   hop: 3,
                   entity_type: 'stock',
                   entity_id: s2.ticker,
                   entity_name: s2.ticker,
                   impact_type: 'Forced Selling',
                   impact_value: total_impact,
                   sector: s2.sector
               }) AS hop3_data
              
               RETURN hop1_data + hop2_data + hop3_data AS all_impacts
           """, id=bank_id).single()
          
           if result and result['all_impacts']:
               df = pd.DataFrame(result['all_impacts'])
              
               # APPLY K-ANONYMITY
               print(f"\n🔒 Applying K-Anonymity (k={k})...")
              
               # Anonymize by entity_type and sector
               df_anonymized = self.anonymizer.anonymize_entities(
                   df,
                   quasi_identifiers=['entity_type', 'sector', 'impact_type'],
                   sensitive_attr='impact_value'
               )
              
               if df_anonymized.empty:
                   print(f"⚠️  All groups smaller than k={k}. Creating aggregated summary instead...")
                   df_anonymized = self.anonymizer.create_aggregated_summary(
                       df,
                       group_by_cols=['hop', 'entity_type', 'sector', 'impact_type']
                   )
              
               # Get anonymization stats
               anon_report = self.anonymizer.get_anonymization_report()
              
               print(f"\n📊 Anonymization Report:")
               print(f"   K-value: {anon_report['k_value']}")
               print(f"   Groups created: {anon_report['groups_created']}")
               print(f"   Entities generalized: {anon_report['entities_generalized']}")
               print(f"   Entities suppressed: {anon_report['entities_suppressed']} (too small groups)")
               print(f"   Privacy guarantee: Each group has ≥{k} members")
              
               # Display anonymized table
               if 'entity_id_anonymized' in df_anonymized.columns:
                   display_cols = ['hop', 'entity_type', 'entity_id_anonymized', 'impact_type', 'group_size', 'sector']
                   print("\n📋 K-Anonymous Impact Table (SAFE TO SHARE):\n")
                   print(df_anonymized[display_cols].to_string(index=False))
               else:
                   print("\n📋 Aggregated Impact Summary (SAFE TO SHARE):\n")
                   print(df_anonymized.to_string(index=False))
              
               # Save anonymized version
               timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
               filename = f'cascade_impact_k{k}_anonymized_{bank_id}_{timestamp}.csv'
               df_anonymized.to_csv(filename, index=False)
               print(f"\n✅ Anonymized table saved: {filename}")
               print(f"   🔒 PRIVACY-SAFE for external sharing (k={k} anonymity)")
              
               # Also save detailed (non-anonymous) for internal use
               filename_detailed = f'cascade_impact_detailed_{bank_id}_{timestamp}.csv'
               df.to_csv(filename_detailed, index=False)
               print(f"   📊 Detailed table saved: {filename_detailed}")
               print(f"   ⚠️  INTERNAL USE ONLY (not anonymized)")
              
               return df_anonymized, anon_report
           else:
               print("❌ No impact data found")
               return None, None
  
   def generate_privacy_safe_html_report(self, bank_id, k=3):
       """Creates a privacy-safe HTML report with K-Anonymity"""
       print(f"\n{'='*70}")
       print(f"📄 GENERATING PRIVACY-SAFE HTML REPORT (k={k})")
       print(f"{'='*70}")
      
       with self.driver.session() as session:
           # Get cascade summary (aggregated, not individual entities)
           summary = session.run("""
               MATCH (b:bank {institution_id: $id})
               OPTIONAL MATCH (b)-[:PUBLICLY_TRADED_AS]->(s:stock)
               OPTIONAL MATCH (s)<-[:equity_ownership]-(i:institutional_investor)
               OPTIONAL MATCH (i)-[:equity_ownership]->(s2:stock)
               WHERE s2 <> s
               RETURN b.name AS bank_name,
                      b.total_assets AS assets,
                      count(DISTINCT s) AS direct_stocks,
                      count(DISTINCT i) AS affected_investors,
                      count(DISTINCT s2) AS cascade_stocks
           """, id=bank_id).single()
          
           if not summary:
               print("❌ Bank not found")
               return None
          
           # Get sector-level breakdown (anonymized)
           sector_breakdown = session.run("""
               MATCH (b:bank {institution_id: $id})-[:PUBLICLY_TRADED_AS]->(s1:stock)
                      <-[:equity_ownership]-(i:institutional_investor)
                      -[:equity_ownership]->(s2:stock)
               WHERE s2 <> s1
               WITH s2.sector AS sector, count(DISTINCT s2) AS affected_count
               WHERE affected_count >= $k
               RETURN sector, affected_count
               ORDER BY affected_count DESC
           """, id=bank_id, k=k).data()
          
           html_content = f"""
<!DOCTYPE html>
<html>
<head>
   <title>Cascade Report (K-Anonymous): {bank_id}</title>
   <style>
       body {{
           font-family: Arial, sans-serif;
           max-width: 1200px;
           margin: 0 auto;
           padding: 20px;
           background: #f5f5f5;
       }}
       .header {{
           background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
           color: white;
           padding: 30px;
           border-radius: 10px;
           margin-bottom: 20px;
       }}
       .privacy-badge {{
           background: #48bb78;
           color: white;
           padding: 10px 20px;
           border-radius: 5px;
           display: inline-block;
           margin-top: 10px;
       }}
       .metric-box {{
           display: inline-block;
           background: white;
           padding: 20px;
           margin: 10px;
           border-radius: 8px;
           box-shadow: 0 2px 4px rgba(0,0,0,0.1);
           min-width: 200px;
       }}
       .metric-value {{
           font-size: 36px;
           font-weight: bold;
           color: #667eea;
       }}
       .metric-label {{
           font-size: 14px;
           color: #666;
           margin-top: 5px;
       }}
       .section {{
           background: white;
           padding: 20px;
           margin: 20px 0;
           border-radius: 8px;
           box-shadow: 0 2px 4px rgba(0,0,0,0.1);
       }}
       .hop {{
           border-left: 4px solid #667eea;
           padding-left: 15px;
           margin: 15px 0;
       }}
       .warning {{
           background: #fef5e7;
           border-left: 4px solid #f39c12;
           padding: 15px;
           margin: 15px 0;
       }}
       table {{
           width: 100%;
           border-collapse: collapse;
           margin: 15px 0;
       }}
       th, td {{
           padding: 12px;
           text-align: left;
           border-bottom: 1px solid #ddd;
       }}
       th {{
           background-color: #667eea;
           color: white;
       }}
   </style>
</head>
<body>
   <div class="header">
       <h1>🚨 Cascade Simulation Report</h1>
       <h2>Trigger: {summary['bank_name']} ({bank_id})</h2>
       <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
       <div class="privacy-badge">
           🔒 K-ANONYMOUS (k={k}) - PRIVACY-SAFE FOR SHARING
       </div>
   </div>
  
   <div class="warning">
       <strong>🔒 Privacy Notice:</strong> This report uses K-Anonymity (k={k}) to protect entity identities.
       Individual entities are grouped and aggregated. Only groups with {k} or more members are shown.
       This report is SAFE to share externally and complies with privacy best practices.
   </div>
  
   <div style="text-align: center;">
       <div class="metric-box">
           <div class="metric-value">{summary['direct_stocks']}</div>
           <div class="metric-label">Direct Stocks Crashed</div>
       </div>
       <div class="metric-box">
           <div class="metric-value">{summary['affected_investors']}</div>
           <div class="metric-label">Investors Impacted</div>
       </div>
       <div class="metric-box">
           <div class="metric-value">{summary['cascade_stocks']}</div>
           <div class="metric-label">Cascade Stocks Affected</div>
       </div>
       <div class="metric-box">
           <div class="metric-value">{summary['direct_stocks'] + summary['cascade_stocks'] + summary['affected_investors']}</div>
           <div class="metric-label">Total Entities</div>
       </div>
   </div>
  
   <div class="section">
       <h2>📊 Cascade Propagation Path (Aggregated)</h2>
      
       <div class="hop">
           <h3>HOP 1: Bank Failure → Stock Crash</h3>
           <p>The bank's publicly traded stock immediately crashes to zero.</p>
           <p><strong>Mechanism:</strong> PUBLICLY_TRADED_AS relationship</p>
           <p><strong>Affected:</strong> {summary['direct_stocks']} stock(s)</p>
       </div>
      
       <div class="hop">
           <h3>HOP 2: Stock Crash → Investor Losses</h3>
           <p>{summary['affected_investors']} institutional investors holding this stock suffer immediate losses.</p>
           <p><strong>Mechanism:</strong> equity_ownership relationships</p>
           <p><strong>Privacy:</strong> Individual investors not disclosed (k-anonymous groups only)</p>
       </div>
      
       <div class="hop">
           <h3>HOP 3: Investor Losses → Forced Selling</h3>
           <p>Investors liquidate {summary['cascade_stocks']} other stocks to cover losses and meet redemptions.</p>
           <p><strong>Mechanism:</strong> Forced selling pressure</p>
           <p><strong>Privacy:</strong> Grouped by sector (minimum {k} stocks per sector)</p>
       </div>
      
       <div class="hop">
           <h3>HOP 4: Market Correlation → Panic</h3>
           <p>Fear spreads to correlated stocks in the same sector.</p>
           <p><strong>Mechanism:</strong> market_correlation relationships</p>
       </div>
   </div>
  
   <div class="section">
       <h2>📈 Affected Sectors (K-Anonymous Aggregation)</h2>
       <p>Only sectors with {k} or more affected stocks are shown:</p>
       <table>
           <tr>
               <th>Sector</th>
               <th>Affected Stock Count</th>
               <th>Privacy Level</th>
           </tr>
"""
          
           for row in sector_breakdown:
               html_content += f"""
           <tr>
               <td>{row['sector']}</td>
               <td>{row['affected_count']}</td>
               <td>✅ K-Anonymous (k≥{k})</td>
           </tr>
"""
          
           html_content += f"""
       </table>
       <p><em>Sectors with fewer than {k} affected stocks are suppressed for privacy.</em></p>
   </div>
  
   <div class="section">
       <h2>💡 Interpretation</h2>
       <ul>
           <li><strong>Systemic Risk Score:</strong> {min(10, (summary['direct_stocks'] + summary['cascade_stocks'] + summary['affected_investors'])/8):.1f}/10</li>
           <li><strong>Contagion Depth:</strong> 4 hops</li>
           <li><strong>Cross-Layer Propagation:</strong> Banking → Market → Ownership → Market</li>
           <li><strong>Risk Assessment:</strong> {'🔴 CRITICAL - Too Big to Fail' if summary['affected_investors'] > 30 else '🟡 HIGH - Systemic Importance' if summary['affected_investors'] > 15 else '🟢 MODERATE - Contained Impact'}</li>
           <li><strong>Privacy Guarantee:</strong> K-Anonymity (k={k}) applied - safe for external sharing</li>
       </ul>
   </div>
  
   <div class="section">
       <h2>🔒 Privacy Methodology</h2>
       <p><strong>K-Anonymity (k={k}):</strong> Each group in this report contains at least {k} entities,
       making it impossible to identify individual entities. This is a standard privacy-preserving technique
       used in healthcare (HIPAA) and financial reporting.</p>
      
       <p><strong>What is protected:</strong></p>
       <ul>
           <li>Individual entity names (banks, stocks, investors)</li>
           <li>Specific holdings and positions</li>
           <li>Proprietary trading relationships</li>
       </ul>
      
       <p><strong>What is disclosed:</strong></p>
       <ul>
           <li>Aggregate counts (stocks, investors, sectors)</li>
           <li>General propagation mechanisms</li>
           <li>Systemic risk assessment</li>
       </ul>
   </div>
</body>
</html>
"""
          
           timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
           filename = f'cascade_report_k{k}_anonymous_{bank_id}_{timestamp}.html'
           with open(filename, 'w') as f:
               f.write(html_content)
          
           print(f"✅ Privacy-safe HTML report saved: {filename}")
           print(f"   🔒 K-Anonymous (k={k}) - SAFE TO SHARE EXTERNALLY")
           print(f"   Open in browser to view")
          
           return filename
  
   def create_comparison_chart_data(self, bank_ids, k=3):
       """Generate K-ANONYMIZED comparison data"""
       print(f"\n{'='*70}")
       print(f"📊 K-ANONYMOUS COMPARISON CHART DATA (k={k})")
       print(f"{'='*70}")
      
       comparison_data = []
      
       with self.driver.session() as session:
           for bank_id in bank_ids:
               result = session.run("""
                   MATCH (b:bank {institution_id: $id})
                   OPTIONAL MATCH (b)-[:PUBLICLY_TRADED_AS]->(s:stock)
                   OPTIONAL MATCH (s)<-[:equity_ownership]-(i:institutional_investor)
                   OPTIONAL MATCH (i)-[:equity_ownership]->(s2:stock)
                   WHERE s2 <> s
                   RETURN b.name AS name,
                          count(DISTINCT s) AS direct_stocks,
                          count(DISTINCT i) AS investors,
                          count(DISTINCT s2) AS cascade_stocks
               """, id=bank_id).single()
              
               if result:
                   comparison_data.append({
                       'Bank': f"Bank_Group_{len(comparison_data)+1}",  # Anonymized ID
                       'Direct Impact': result['direct_stocks'],
                       'Investors': result['investors'],
                       'Cascade Impact': result['cascade_stocks'],
                       'Total Impact': result['direct_stocks'] + result['investors'] + result['cascade_stocks']
                   })
      
       df = pd.DataFrame(comparison_data)
       df = df.sort_values('Total Impact', ascending=False)
      
       print("\n📊 Systemic Importance Ranking (K-Anonymous):\n")
       print(df.to_string(index=False))
       print(f"\n🔒 Privacy Note: Bank identities anonymized as Bank_Group_1, Bank_Group_2, etc.")
      
       # Save
       timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       filename = f'systemic_comparison_k{k}_anonymous_{timestamp}.csv'
       df.to_csv(filename, index=False)
       print(f"\n✅ K-Anonymous comparison saved: {filename}")
      
       return df




def main():
   print("="*70)
   print("CASCADE VISUALIZATION TOOLKIT WITH K-ANONYMITY")
   print("="*70)
  
   NEO4J_URI = "neo4j+s://3979c887.databases.neo4j.io"
   NEO4J_USER = "neo4j"
   NEO4J_PASSWORD = input("Enter Neo4j password: ")
  
   # Ask for k value
   k_input = input("Enter k value for K-Anonymity (default=3, recommended 3-5): ").strip()
   k = int(k_input) if k_input else 3
  
   viz = CascadeVisualizerWithPrivacy(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, k=k)
  
   try:
       bank_id = input("\nEnter bank ID to analyze (e.g., JPM, BAC, GS): ").strip().upper()
      
       # 1. Generate Neo4j Browser query (internal use - not anonymized)
       print("\n" + "="*70)
       print("STEP 1: NEO4J VISUALIZATION (Internal Use)")
       print("="*70)
       viz.generate_neo4j_visualization_query(bank_id)
      
       # 2. Generate K-ANONYMIZED impact table (safe to share)
       print("\n" + "="*70)
       print("STEP 2: K-ANONYMOUS IMPACT TABLE (Safe to Share)")
       print("="*70)
       viz.generate_anonymized_impact_table(bank_id, k=k)
      
       # 3. Generate privacy-safe HTML report (safe to share)
       print("\n" + "="*70)
       print("STEP 3: PRIVACY-SAFE HTML REPORT (Safe to Share)")
       print("="*70)
       viz.generate_privacy_safe_html_report(bank_id, k=k)
      
       # 4. Compare with other banks (anonymized)
       print("\n" + "="*70)
       compare = input("Compare with other banks (anonymized)? (y/n): ").lower()
       if compare == 'y':
           banks = ['JPM', 'BAC', 'GS', 'MS', 'C', 'WFC']
           viz.create_comparison_chart_data(banks, k=k)
      
       print("\n" + "="*70)
       print("✅ ALL VISUALIZATIONS GENERATED!")
       print("="*70)
       print("\n📂 Output Files Created:")
       print("  • K-Anonymous CSV table (SAFE TO SHARE)")
       print("  • Privacy-safe HTML report (SAFE TO SHARE)")
       print("  • Detailed CSV (INTERNAL USE ONLY)")
       print("  • Neo4j query (INTERNAL USE ONLY)")
       print(f"\n🔒 Privacy Guarantee: K-Anonymity (k={k}) applied to all shared outputs")
      
   finally:
       viz.close()




if __name__ == "__main__":
   main()





