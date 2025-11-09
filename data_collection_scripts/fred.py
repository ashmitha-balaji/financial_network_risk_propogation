"""
PART 4: FRED Economic Data Collection & Dataset Integration
Estimated Time: 8–10 hours
Target: 10 000 + rows of economic indicators + complete integration
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pathlib import Path
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FREDDataCollector:
    """
    Collects economic indicators from FRED (Federal Reserve Economic Data)
    """

    def __init__(self, api_key, output_dir="data/economic"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://api.stlouisfed.org/fred"

        # ---- Key FRED series for macro stress analysis ----
        self.series_ids = {
            # Interest & yield
            "FEDFUNDS": "Federal Funds Rate",
            "DGS10": "10-Year Treasury Rate",
            "DGS2": "2-Year Treasury Rate",
            "T10Y2Y": "10-Year minus 2-Year Spread",

            # Credit & liquidity
            "BAMLH0A0HYM2": "High Yield Credit Spread",
            "TEDRATE": "TED Spread",
            "VIXCLS": "CBOE Volatility Index",

            # Labor & macro
            "UNRATE": "Unemployment Rate",
            "CPIAUCSL": "Consumer Price Index",
            "GDPC1": "Real GDP",
            "PAYEMS": "Nonfarm Payroll Employment",
            "INDPRO": "Industrial Production Index",
        }

    # ------------------------------------------------------
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

    # ------------------------------------------------------
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

    # ------------------------------------------------------
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

    # ------------------------------------------------------
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


# ----------------- Stand-alone run -----------------
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("❌ FRED_API_KEY missing in .env")

    collector = FREDDataCollector(api_key)
    econ = collector.collect_all("2020-01-01")
    stress = collector.compute_stress_index(econ)
    collector.save_outputs(econ, stress)
    print("\n✅ FRED economic layer collection complete !")