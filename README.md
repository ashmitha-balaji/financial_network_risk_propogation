# financial_network_risk_propogation
Bigdata Project 


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

**Nodes (unified_nodes.csv):**
Banks: synthetic + FDIC (if available)
Stocks: all companies with fundamentals
Institutional investors: unique CIKs from 13F holdings

**Edges (unified_edges.csv):**
Interbank lending (bank → bank)
Market correlations (stock → stock)
Ownership (institution → stock)
