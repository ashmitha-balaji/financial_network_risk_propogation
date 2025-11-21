#!/usr/bin/env python3
"""
Combined banking collector + streaming server (file-drop mode).
Generates upscaled banking data and streams indicators/lending events.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import fredapi
from dotenv import load_dotenv

# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ----------------- Load .env -----------------
ENV_PATH = "/Users/ashmitharoopkumar/Desktop/financial_network_analysis_backup/.env"
load_dotenv(dotenv_path=ENV_PATH)

OUTPUT_DIR = Path("/tmp/streams/banking")


class UpscaledBankingLayerCollector:
    """UPSCALED Banking Layer Collector"""

    def __init__(self):
        api_key = os.getenv("FRED_API_KEY")
        if not api_key:
            raise ValueError("❌ FRED_API_KEY not found in .env file")

        self.fred = fredapi.Fred(api_key=api_key)

        # Tiered institutions
        self.tier1_banks = [
            "JPM", "BAC", "WFC", "C", "USB", "PNC", "TFC", "COF",
            "BK", "STT", "MTB", "FITB", "KEY", "CFG", "RF", "HBAN",
            "ZION", "CMA", "FHN", "WTFC"
        ]
        self.tier2_investment = [
            "GS", "MS", "SCHW", "BLK", "TROW", "BEN", "IVZ", "APAM",
            "SEIC", "AMG", "VRTS", "SIVB", "SBNY", "NYCB", "WAL"
        ]
        self.tier3_regional = [
            "FCNCA", "CATY", "UCBI", "IBOC", "BANC", "TCBI", "UMBF",
            "OZK", "EWBC", "WAFD", "CASH", "FNB", "CVBF", "PNFP", "SNV"
        ]
        self.tier4_credit_unions = [f"CU_{i:03d}" for i in range(1, 51)]

        self.all_institutions = (
            self.tier1_banks + self.tier2_investment + self.tier3_regional + self.tier4_credit_unions
        )

        # Expanded FRED indicators
        self.banking_indicators = {
            "fed_funds_rate": "DFF",
            "prime_rate": "DPRIME",
            "libor_3m": "SOFR",
            "treasury_10y": "DGS10",
            "treasury_2y": "DGS2",
            "yield_spread": "T10Y2Y",
            "total_lending": "TOTLL",
            "commercial_loans": "TOTCI",
            "consumer_loans": "CONSUMER",
            "real_estate_loans": "REALLN",
            "bank_assets": "TLAACBW027SBOG",
            "deposits": "DPSACBW027SBOG",
            "vix": "VIXCLS",
            "credit_spread": "BAMLH0A0HYM2",
            "mortgage_rate": "MORTGAGE30US",
        }

        logger.info(f"✅ Banking Layer Collector initialized with {len(self.all_institutions)} institutions")

    # ----------------- Data generation -----------------

    def collect_banking_indicators_timeseries(self, lookback_days=730):
        """Collect timeseries banking indicators."""
        logger.info(f"📊 Collecting {lookback_days} days of banking indicators...")
        start_date = datetime.now() - timedelta(days=lookback_days)
        all_timeseries = []

        for name, series_id in self.banking_indicators.items():
            try:
                data = self.fred.get_series(series_id, observation_start=start_date)
                if not data.empty:
                    for date, value in data.items():
                        all_timeseries.append({
                            "date": pd.to_datetime(date).strftime("%Y-%m-%d"),
                            "indicator_name": name,
                            "series_id": series_id,
                            "value": float(value),
                            "layer": "banking",
                        })
                    logger.info(f"  ✓ {name}: {len(data)} observations")
                else:
                    logger.warning(f"  ⚠️ {name}: No data available")
            except Exception as e:
                logger.error(f"  ❌ Error fetching {name}: {e}")
            time.sleep(0.5)

        df = pd.DataFrame(all_timeseries)
        logger.info(f"✅ Collected {len(df):,} indicator observations")
        return df

    def generate_interbank_lending_network(self, num_transactions=15000):
        """Generate interbank lending relationships."""
        logger.info(f"🏦 Generating {num_transactions:,} interbank lending transactions...")
        np.random.seed(42)
        relationships = []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        for i in range(num_transactions):
            lender = np.random.choice(self.all_institutions)
            borrower = np.random.choice([b for b in self.all_institutions if b != lender])

            # Base amount by tier
            if lender in self.tier1_banks:
                base_amount = 5e9
            elif lender in self.tier2_investment:
                base_amount = 2e9
            elif lender in self.tier3_regional:
                base_amount = 5e8
            else:
                base_amount = 1e8

            rel_type = np.random.choice([
                "interbank_lending", "overnight_lending", "repo_agreement",
                "reverse_repo", "derivative_exposure", "credit_line",
                "correspondent_banking", "letter_of_credit", "syndicated_loan",
                "fed_funds_transaction"
            ], p=[0.25, 0.15, 0.15, 0.10, 0.10, 0.10, 0.05, 0.05, 0.03, 0.02])

            transaction_date = np.random.choice(date_range)
            transaction_date = pd.to_datetime(transaction_date).to_pydatetime()

            relationships.append({
                "transaction_id": f"TXN_{i+1:06d}",
                "transaction_date": transaction_date.strftime("%Y-%m-%d"),
                "from_institution": lender,
                "to_institution": borrower,
                "relationship_type": rel_type,
                "amount_usd": np.random.uniform(base_amount * 0.5, base_amount * 2),
                "currency": "USD",
                "maturity_days": np.random.randint(1, 365),
                "interest_rate": np.random.uniform(0.5, 6.5),
                "risk_weight": round(np.random.uniform(0.1, 0.9), 3),
                "collateral_type": np.random.choice(["cash", "treasury", "mortgage_backed", "corporate_bond", "unsecured"]),
                "layer": "banking",
            })

            if (i + 1) % 5000 == 0:
                logger.info(f"  Generated {i+1:,}/{num_transactions:,} transactions...")

        df = pd.DataFrame(relationships)
        logger.info(f"✅ Generated {len(df):,} interbank lending relationships")
        return df

    def generate_institution_profiles(self):
        """Generate institution profiles."""
        logger.info(f"🏢 Generating profiles for {len(self.all_institutions)} institutions...")
        profiles = []

        for institution in self.all_institutions:
            tier = (
                "tier1_national" if institution in self.tier1_banks else
                "tier2_investment" if institution in self.tier2_investment else
                "tier3_regional" if institution in self.tier3_regional else
                "tier4_credit_union"
            )

            assets = np.random.uniform(1e9, 3e12)
            deposits = assets * np.random.uniform(0.4, 0.9)
            num_branches = np.random.randint(5, 5000)

            profiles.append({
                "institution_id": institution,
                "institution_name": f"{institution} Financial Corp",
                "tier": tier,
                "total_assets": assets,
                "total_deposits": deposits,
                "total_loans": assets * np.random.uniform(0.5, 0.7),
                "equity": assets * np.random.uniform(0.08, 0.12),
                "num_branches": num_branches,
                "num_employees": num_branches * np.random.randint(10, 50),
                "layer": "banking",
            })

        df = pd.DataFrame(profiles)
        logger.info(f"✅ Generated {len(df)} institution profiles")
        return df

    def generate_stress_events(self, num_events=500):
        """Generate stress events."""
        logger.info(f"⚠️ Generating {num_events} stress events...")
        events = []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)
        event_types = [
            "liquidity_crunch", "credit_downgrade", "loan_default", "regulatory_action",
            "fraud_discovery", "cyber_attack", "market_crash", "interest_rate_shock",
            "currency_crisis", "real_estate_crash"
        ]

        for i in range(num_events):
            event_date = start_date + timedelta(days=np.random.randint(0, 730))
            affected_institution = np.random.choice(self.all_institutions)
            event_type = np.random.choice(event_types)
            severity = np.random.uniform(0.3, 1.0)
            financial_impact = np.random.uniform(1e6, 1e10)

            events.append({
                "event_id": f"EVENT_{i+1:04d}",
                "event_date": event_date.strftime("%Y-%m-%d"),
                "affected_institution": affected_institution,
                "event_type": event_type,
                "severity_score": round(severity, 3),
                "financial_impact_usd": financial_impact,
                "resolved": np.random.choice([True, False], p=[0.8, 0.2]),
                "resolution_days": np.random.randint(1, 180),
                "contagion_risk": round(np.random.uniform(0.1, 0.9), 3),
                "layer": "banking",
            })

        df = pd.DataFrame(events)
        logger.info(f"✅ Generated {len(df)} stress events")
        return df

    def generate_daily_positions(self, num_days=365, institutions_sample=30):
        """Generate daily positions."""
        logger.info(f"📅 Generating daily positions for {institutions_sample} institutions over {num_days} days...")
        positions = []

        sampled_institutions = np.random.choice(self.all_institutions, size=institutions_sample, replace=False)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=num_days)
        date_range = pd.date_range(start=start_date, end=end_date)

        for institution in sampled_institutions:
            base_assets = np.random.uniform(10e9, 500e9)
            for date in date_range:
                daily_change = np.random.uniform(-0.02, 0.02)
                current_assets = base_assets * (1 + daily_change)
                positions.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "institution_id": institution,
                    "total_assets": current_assets,
                    "cash_equivalents": current_assets * np.random.uniform(0.05, 0.15),
                    "securities": current_assets * np.random.uniform(0.15, 0.25),
                    "loans": current_assets * np.random.uniform(0.50, 0.65),
                    "other_assets": current_assets * np.random.uniform(0.05, 0.10),
                    "total_liabilities": current_assets * np.random.uniform(0.88, 0.92),
                    "deposits": current_assets * np.random.uniform(0.60, 0.75),
                    "borrowings": current_assets * np.random.uniform(0.10, 0.20),
                    "equity": current_assets * np.random.uniform(0.08, 0.12),
                    "layer": "banking",
                })

        df = pd.DataFrame(positions)
        logger.info(f"✅ Generated {len(df):,} daily position records")
        return df

    def save_all_data(self, datasets):
        """Save all datasets to /data/banking_upscaled with timestamps."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)
        data_dir = os.path.join(parent_dir, "data", "banking_upscaled")
        os.makedirs(data_dir, exist_ok=True)

        saved_files = []
        for name, df in datasets.items():
            filename = os.path.join(data_dir, f"{name}_{timestamp}.csv")
            df.to_csv(filename, index=False)
            saved_files.append(filename)
            logger.info(f"💾 Saved {name}: {len(df):,} rows → {filename}")

        summary_file = os.path.join(data_dir, f"collection_summary_{timestamp}.json")
        with open(summary_file, "w") as f:
            json.dump({name: len(df) for name, df in datasets.items()}, f, indent=2)

        return saved_files, summary_file

    def print_summary(self, datasets):
        print("\n" + "=" * 80)
        print("UPSCALED BANKING LAYER DATA COLLECTION COMPLETE")
        print("=" * 80)
        total_rows = 0
        for name, df in datasets.items():
            print(f"{name}: {len(df):,} rows, {len(df.columns)} columns")
            total_rows += len(df)
        print(f"TOTAL ROWS: {total_rows:,}")


# ----------------- Streaming helpers -----------------

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def df_to_events(df: pd.DataFrame, source: str, subtype: str):
    for _, row in df.iterrows():
        record = row.to_dict()
        record["source_layer"] = source
        record["subtype"] = subtype
        yield record


def stream_banking(lookback_days: int = 3, num_transactions: int = 200, sleep_sec: int = 1):
    logger.info("🔌 Banking streaming server starting (file-drop mode)...")
    ensure_dir(OUTPUT_DIR)

    collector = UpscaledBankingLayerCollector()
    logger.info("✅ Banking Layer Collector initialized")

    logger.info(f"📊 Collecting {lookback_days} days of banking indicators...")
    df_indicators = collector.collect_banking_indicators_timeseries(lookback_days=lookback_days)

    logger.info(f"🏦 Generating {num_transactions} interbank lending transactions...")
    df_lending = collector.generate_interbank_lending_network(num_transactions=num_transactions)

    logger.info(
        f"✅ Banking indicators: {len(df_indicators)} rows, "
        f"interbank lending: {len(df_lending)} rows"
    )

    for event in df_to_events(df_indicators, "banking", "indicator"):
        ts = int(time.time() * 1000)
        fname = OUTPUT_DIR / f"indicator_{ts}.json"
        with open(fname, "w") as f:
            json.dump(event, f, default=str)
        logger.info("Streaming indicators to Spark")
        time.sleep(sleep_sec)

    for event in df_to_events(df_lending, "banking", "interbank_edge"):
        ts = int(time.time() * 1000)
        edge_id = event.get("transaction_id", "tx")
        fname = OUTPUT_DIR / f"lending_{ts}_{edge_id}.json"
        with open(fname, "w") as f:
            json.dump(event, f, default=str)
        logger.info(f"📨 Wrote lending edge → {fname}")
        time.sleep(sleep_sec)

    logger.info("✅ Banking streaming complete.")


def main():
    parser = argparse.ArgumentParser(description="Banking collector/streaming (file-drop).")
    parser.add_argument("--mode", choices=["stream", "collect"], default="stream", help="stream events to file-drop or collect full datasets to disk")
    parser.add_argument("--lookback-days", type=int, default=3, help="Days of indicators to stream")
    parser.add_argument("--num-transactions", type=int, default=200, help="Number of lending transactions to stream")
    parser.add_argument("--sleep-sec", type=float, default=1.0, help="Sleep between events when streaming")
    args = parser.parse_args()

    if args.mode == "collect":
        collector = UpscaledBankingLayerCollector()
        datasets = {
            "banking_indicators_timeseries": collector.collect_banking_indicators_timeseries(730),
            "interbank_lending_network": collector.generate_interbank_lending_network(15000),
            "institution_profiles": collector.generate_institution_profiles(),
            "stress_events": collector.generate_stress_events(500),
            "daily_positions": collector.generate_daily_positions(365, 30),
        }
        saved_files, summary_file = collector.save_all_data(datasets)
        collector.print_summary(datasets)
        print(f"\n📁 Data saved to: {os.path.dirname(saved_files[0])}")
        print(f"📄 Summary: {summary_file}")
    else:
        stream_banking(
            lookback_days=args.lookback_days,
            num_transactions=args.num_transactions,
            sleep_sec=args.sleep_sec,
        )


if __name__ == "__main__":
    main()
