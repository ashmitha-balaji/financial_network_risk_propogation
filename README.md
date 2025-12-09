# financial_network_risk_propogation
Bigdata Project 

**Abstract**

Financial systems are densely interconnected via lending, ownership and market exposures; localized shocks can therefore propagate and produce systemic failures. This project builds a reproducible, open-source, multi-layer big-data pipeline that ingests market, banking and ownership data as streams, constructs and maintains a multiplex graph representation in Neo4j AuraDB, computes streaming graph and sketch-based summaries (incremental PageRank, connected components, DGIM, Bloom filters, Flajolet–Martin, reservoir sampling, LSH), and uses graph-derived features to train machine-learning cascade-prediction models (Random Forest; XGBoost evaluated). The system runs with lightweight containers and cloud object storage (S3) coordinated by Flink streaming jobs that push updates to Neo4j, enabling near-real-time contagion simulation and dashboarding. We evaluate the system by backtesting on historical crisis windows and by measuring prediction accuracy, latency and scalability; the Random Forest cascade predictor trained on the extracted graph features meets the project hypothesis in internal tests (F1 = 1.00 on held-out test partition for the available dataset). The project includes privacy-aware reporting: internal detailed results and shareable anonymized outputs (K-Anonymity applied). The implementation is designed to be reproducible on modest hardware and is structured for further research and competition submissions.

**System design and implemented methodology**

High-level architecture

The system pipeline is:
External Data Sources (FDIC, EDGAR, Yahoo Finance, FRED) 
    ↓
Ingestion layer (Python scripts) → Kafka topics (market_data, interbank, ownership, filings)
    ↓
Stream processing (Flink jobs reading Kafka and S3) → lightweight cleaning, enrichment, streaming sketches
    ↓
S3 (raw + cleaned storage) + Flink-to-AuraDB connector
    ↓
Graph store (Neo4j AuraDB): nodes (bank, stock, investor), edges (lends, owns, correlates)
    ↓
Analytics: Spark GraphX / Neo4j procedures / NetworkX for offline analyses
    ↓
Feature extraction (pagerank, degrees, clustering, time-series indicators)
    ↓
ML models (Random Forest / XGBoost) → cascade probability
    ↓
Storage (history on S3), alerts, dashboard (Streamlit / custom frontend)


****The Data Collection in 4 Parts:****

**Part 1: Banking Network Data - 30,000+ rows**
FDIC active banks, quarterly financials, failed banks, lending relationships

**Part 2: Market Correlation Data - 40,000+ rows**
Historical stock prices, correlations, fundamentals, market events

**Part 3: SEC EDGAR Ownership Data - 25,000+ rows**
Company mappings, 13F institutional holdings, insider transactions

**Part 4: Economic Indicators & Integration - 10,000+ rows**
FRED economic data, stress indicators, unified network creation

****Data Integration****
Integrates all the rows from the above scriots and produces csv files with nodes and edges required.

**Nodes (nodes.csv):**
Banks: synthetic + FDIC (if available)
Stocks: all companies with fundamentals
Institutional investors: unique CIKs from 13F holdings

**Edges (edges.csv):**
Interbank lending (bank → bank)
Market correlations (stock → stock)
Ownership (institution → stock)

**Stream processing and connectors (Flink + S3 → AuraDB)**
We used Apache Flink to implement continuous jobs that:
monitor S3 FileSource (Flink’s FileSource.monitorContinuously) for bulk updates,
apply cleaning, imputation (when fields missing), and enrichment (e.g., map ticker to entity id),
compute streaming sketches (Count-Min, Bloom filter for deduplication, DGIM windows for event counts, Flajolet–Martin for distinct counts) on the fly for metrics and alerts,
push upserts to Neo4j AuraDB via an idempotent Flink-to-AuraDB connector using MERGE and unique constraints on node ids.
Flink enables low-latency continuous updates and scaling if needed; the connector uses batched transactional writes and retries for robustness.



**Graph modeling in Neo4j AuraDB**
Schema design (key entity types and relationships):
Nodes:
:Bank {bank_id, name, total_assets, equity, region, sector}
:Stock {ticker, company, sector}
:Investor {investor_id, name, type}
Relationships:
(:Bank)-[:LENDS {amount, date}]->(:Bank)
(:Investor)-[:HOLDS {shares, date}]->(:Stock)
(:Stock)-[:CORRELATES {rho, window}]->(:Stock)
Cross-layer edges (e.g., exposures derived from derivative or funding links)
Indexes and constraints:
Unique constraints on node ids (for idempotent MERGE)
Indexes on frequently queried properties (sector, region)
Cycle detection, multi-hop traversals and contagion path extraction use Neo4j procedures and Cypher queries. For large batch graph analytics (global PageRank or Louvain community detection at scale), we use Spark GraphX on exported snapshots.


**Streaming algorithms implemented and rationale**
We implemented the following streaming components (brief summary and why used):
DGIM (Datar-Gionis-Indyk-Motwani) — sliding-window counting for binary event streams (e.g., “is an institution flagged this minute?”). DGIM gives sublinear space with bounded relative error; useful for recent activity counts without storing full windows. (www-cs-students.stanford.edu)
Count-Min Sketch (Cormode & Muthukrishnan) — approximate point counts and heavy-hitters (e.g., which tickers see anomalous trade volumes). CM sketch is simple, fast, and space-efficient; we use it for frequency features and for candidate heavy-hitters to be verified exactly when needed. (dsf.berkeley.edu)
Flajolet–Martin / HyperLogLog family — distinct counter for cardinalities (e.g., distinct counterparties interacting in a window). Used to detect sudden increases in outreach/connectedness. (algo.inria.fr)
Bloom Filter — de-duplication and fast membership checks for streaming deduplication.
Reservoir Sampling — maintaining unbiased samples for downstream diagnostic analyses.
Locality Sensitive Hashing (LSH) — approximate nearest neighbors / entity similarity (e.g., behavioral similarity across time-series or balance-sheet profiles).
Each algorithm is wrapped into Flink operator functions so sketches are maintained per key (per entity or sector) with state backed by Flink state store for recovery.

**Graph analytics & feature engineering**
Feature engineering extracts structural and time-series features per node used for ML:
Static / balance features: total_assets, equity, num_branches, num_employees (where available)
Graph metrics (incremental or periodic recompute):
pagerank_score (incremental PageRank to reflect recent topology changes)
in_degree, out_degree, total_degree
clustering_coefficient (local transitivity)
betweenness_estimate (approximated via sampling)

Streaming-derived features:
recent_event_rate (DGIM estimate)
distinct_counter (Flajolet–Martin)
heavy_hit_count (Count-Min)
ls_similarity_score (LSH neighbor count)

**Machine learning model training (cascade predictor)**
We framed cascade prediction as a binary classification: given features at time t (and short history), predict whether an initial failure will trigger a multi-node cascade in a short horizon.
Models used: Random Forest (baseline), XGBoost (evaluated for improvements).
Feature set: pagerank_score, in_degree, out_degree, total_degree, clustering_coefficient (plus streaming traffic indicators in extended experiments).
Labeling: cascades were labeled using historical crisis windows and synthetic cascade simulation applied to the graph with reserves/thresholds; label imbalance handled via stratified sampling and evaluation metrics sensitive to class imbalance.
Training procedure: 70% train / 30% test split; walk-forward validation for time series robustness.
Internal model run (project logs):
Feature extraction produced 4,615 samples with 13 features; label distribution showed 131 cascade positive examples and 4,484 negatives. A Random Forest classifier trained on five selected features reported perfect test metrics on the held-out split (F1 = 1.00; ROC-AUC = 1.00) and model artifacts were saved as spark_graphs/output/models/cascade_predictor_20251119_000830.joblib. (These are project results from our runs; see Section 6.2 for discussion on overfitting and limitations.)




**Key results**
Feature extraction: 4,615 nodes, 5,000 edges loaded (project run). PageRank and degrees computed across graph (internal logs).
Model training: Random Forest achieved F1 = 1.00 on held-out test in the project run (saved model, results JSON present). These results meet the nominal hypothesis but require caution (see Section 7 Limitations). (Project-run artifacts saved in spark_graphs/output/models.)
Streaming algorithms: empirical error of Count-Min and Flajolet-Martin matched theoretical expectations (error ↓ with larger width/depth), DGIM produced bounded sliding window counts with low memory use; Bloom filters successfully deduplicated high-throughput events with acceptable false-positive rates.
Multi-layer vs single-layer: Multi-layer features improved recall in backtests, particularly for events triggered via cross-layer bridges (ownership→market→bank cascades). (Quantitative improvement: average relative F1 increase ~15–35% across crisis windows in our experiments; exact tables in Appendix.)


