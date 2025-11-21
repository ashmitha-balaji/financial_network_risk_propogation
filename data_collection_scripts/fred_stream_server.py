#!/usr/bin/env python3
"""
Combined FRED collector + streaming server (file-drop mode).
Fetches economic indicators, computes stress index, and streams JSON rows.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("FREDStream")

OUTPUT_DIR = Path("/tmp/stream_fred")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Load .env at project root if present
ROOT_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
if ROOT_ENV_PATH.exists():
    load_dotenv(ROOT_ENV_PATH)


class FREDDataCollector:
    """
    Collects economic indicators from FRED (Federal Reserve Economic Data)
    """

    def __init__(self, api_key, output_dir="data/economic"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://api.stlouisfed.org/fred"

        # Key FRED series for macro stress analysis
        self.series_ids = {
            "FEDFUNDS": "Federal Funds Rate",
            "DGS10": "10-Year Treasury Rate",
            "DGS2": "2-Year Treasury Rate",
            "T10Y2Y": "10-Year minus 2-Year Spread",
            "BAMLH0A0HYM2": "High Yield Credit Spread",
            "TEDRATE": "TED Spread",
            "VIXCLS": "CBOE Volatility Index",
            "UNRATE": "Unemployment Rate",
            "CPIAUCSL": "Consumer Price Index",
            "GDPC1": "Real GDP",
            "PAYEMS": "Nonfarm Payroll Employment",
            "INDPRO": "Industrial Production Index",
        }

    def fetch_series(self, series_id, start_date="2020-01-01"):
        """Download one time series from FRED API"""
        url = (
            f"{self.base_url}/series/observations?"
            f"series_id={series_id}&api_key={self.api_key}&file_type=json"
            f"&observation_start={start_date}"
        )
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json().get("observations", [])
            df = pd.DataFrame(data)
            if not df.empty:
                df = df[["date", "value"]]
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df["series_id"] = series_id
            time.sleep(0.5)
            return df
        except Exception as e:
            logger.error(f"Failed {series_id}: {e}")
            return pd.DataFrame(columns=["date", "value", "series_id"])

    def collect_all(self, start_date="2020-01-01"):
        """Loop through all indicators and concatenate"""
        all_df = []
        for sid, desc in self.series_ids.items():
            logger.info(f"Fetching {sid} – {desc}")
            df = self.fetch_series(sid, start_date)
            if not df.empty:
                df["description"] = desc
                all_df.append(df)
        final = pd.concat(all_df, ignore_index=True)
        logger.info(f"✅ Collected {len(final):,} rows from {len(self.series_ids)} series")
        return final

    def compute_stress_index(self, df):
        """Derive composite financial stress score"""
        df_pivot = df.pivot(index="date", columns="series_id", values="value").fillna(method="ffill")
        df_pivot["yield_curve"] = df_pivot["T10Y2Y"]
        df_pivot["stress_score"] = 0
        df_pivot.loc[df_pivot["yield_curve"] < 0, "stress_score"] += 2
        df_pivot.loc[df_pivot["VIXCLS"] > 30, "stress_score"] += 2
        df_pivot.loc[df_pivot["BAMLH0A0HYM2"] > 7, "stress_score"] += 2
        df_pivot["high_stress_flag"] = (df_pivot["stress_score"] >= 4).astype(int)
        logger.info(f"Stress index computed for {len(df_pivot):,} dates")
        return df_pivot.reset_index()

    def save_outputs(self, econ_df, stress_df):
        """Save to CSV and JSON"""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        econ_path = self.output_dir / f"fred_economic_indicators_{ts}.csv"
        stress_path = self.output_dir / f"financial_stress_index_{ts}.csv"
        econ_df.to_csv(econ_path, index=False)
        stress_df.to_csv(stress_path, index=False)
        logger.info(f"💾 Saved economic data → {econ_path}")
        logger.info(f"💾 Saved stress index → {stress_path}")
        return econ_path, stress_path


# ----------------- Streaming helpers -----------------

def write_json(df, prefix="fred"):
    """Write rows of dataframe as individual JSON files for Spark streaming."""
    for _, row in df.iterrows():
        ts = row.get("date", datetime.utcnow().isoformat())
        filename = f"{prefix}_{ts.replace(':', '-')}.json"
        out_path = OUTPUT_DIR / filename

        with open(out_path, "w") as f:
            json.dump(row.to_dict(), f, default=str)

        logger.info("Streaming FRED JSON → Spark")


def stream_fred(api_key, start_date="2020-01-01", sleep_seconds=60):
    if not api_key:
        raise ValueError("❌ Missing FRED_API_KEY in environment variables.")

    logger.info("🔌 FRED streaming server starting (file-drop mode)...")
    collector = FREDDataCollector(api_key)

    while True:
        logger.info("📡 Collecting macroeconomic indicators from FRED...")
        df_econ = collector.collect_all(start_date)
        df_stress = collector.compute_stress_index(df_econ)
        write_json(df_econ, prefix="fred_econ")
        write_json(df_stress, prefix="fred_stress")
        logger.info(f"⏳ Waiting {sleep_seconds} seconds before next FRED update...")
        time.sleep(sleep_seconds)


def main():
    parser = argparse.ArgumentParser(description="FRED collector/streaming (file-drop).")
    parser.add_argument("--mode", choices=["stream", "collect"], default="stream", help="stream JSON to file-drop or collect full datasets to disk")
    parser.add_argument("--api-key", default=os.getenv("FRED_API_KEY", ""), help="FRED API key (falls back to env FRED_API_KEY)")
    parser.add_argument("--start-date", default="2020-01-01", help="Start date for fetching series")
    parser.add_argument("--sleep-seconds", type=int, default=60, help="Sleep between streaming iterations")
    parser.add_argument("--output-dir", default="data/economic", help="Output directory for collected CSVs (collect mode)")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("FRED_API_KEY")

    if args.mode == "collect":
        collector = FREDDataCollector(api_key, output_dir=args.output_dir)
        econ = collector.collect_all(args.start_date)
        stress = collector.compute_stress_index(econ)
        econ_path, stress_path = collector.save_outputs(econ, stress)
        print("\n✅ FRED economic layer collection complete!")
        print(f"    Economic data: {econ_path}")
        print(f"    Stress index: {stress_path}")
    else:
        stream_fred(api_key, start_date=args.start_date, sleep_seconds=args.sleep_seconds)


if __name__ == "__main__":
    main()
