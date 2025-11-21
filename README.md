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

**Nodes (nodes.csv):**
Banks: synthetic + FDIC (if available)
Stocks: all companies with fundamentals
Institutional investors: unique CIKs from 13F holdings

**Edges (edges.csv):**
Interbank lending (bank → bank)
Market correlations (stock → stock)
Ownership (institution → stock)

**Data Collection
**
Requirements to run producer scripts:

1)Create pyenv:
source stream_env/bin/activate

2)Install below inside the pyenv environment
fredapi  - pip3 install fredapi
dotenv  -  pip3 install dotenv
bs4  -  pip3 install bs4
yfinance -  pip3 install yfinance
Pyspark- pip3 install pyspark

