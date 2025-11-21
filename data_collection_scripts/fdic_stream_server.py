#!/usr/bin/env python3
"""
Combined FDIC data collector + streaming server (file-drop mode).
Generates FDIC banking datasets and streams active bank/financial/lending JSONs.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("/tmp/stream_fdic")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class FDICDataCollector:
    """
    Collects banking data from FDIC sources with robust error handling
    """

    def __init__(self, output_dir="data/banking"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://banks.data.fdic.gov/api"

    def _extract_record_data(self, record):
        """Extract actual data from potentially nested record structure"""
        if isinstance(record, dict):
            if "data" in record and isinstance(record["data"], dict):
                return record["data"]
            return record
        return {}

    def collect_active_banks(self):
        """Collect all active FDIC-insured institutions"""
        logger.info("Starting active banks collection...")

        all_banks = []
        offset = 0
        limit = 1000
        max_retries = 3

        while True:
            url = f"{self.base_url}/institutions"
            params = {
                "filters": "ACTIVE:1",
                "fields": "NAME,CERT,ASSET,DEP,DEPDOM,ROA,ROE,NETINC,CITY,STNAME,STALP,ZIP,DATEUPDT,OFFICES",
                "sort_by": "ASSET",
                "sort_order": "DESC",
                "limit": limit,
                "offset": offset,
                "format": "json",
            }

            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()

                    if "data" not in data or len(data["data"]) == 0:
                        logger.info("No more data to fetch")
                        success = True
                        break

                    batch_count = 0
                    for record in data["data"]:
                        extracted_data = self._extract_record_data(record)
                        if extracted_data:
                            all_banks.append(extracted_data)
                            batch_count += 1
                        else:
                            logger.warning(f"Failed to extract data from record: {record}")

                    logger.info(f"Collected {len(all_banks)} banks so far... (batch: {batch_count})")
                    success = True

                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    logger.warning(f"Request failed (attempt {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        time.sleep(2 ** retry_count)
                    else:
                        logger.error(f"Failed after {max_retries} attempts")
                        break
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    break

            if not success or len(data.get("data", [])) == 0:
                break

            offset += limit
            time.sleep(1)

        if len(all_banks) == 0:
            logger.error("No banks collected! API may have changed.")
            return pd.DataFrame()

        logger.info(f"Creating DataFrame from {len(all_banks)} records...")
        df_banks = pd.DataFrame(all_banks)

        if "data" in df_banks.columns and not df_banks["data"].empty and isinstance(df_banks["data"].iloc[0], dict):
            logger.info("Detected nested 'data' column - expanding...")
            df_expanded = pd.json_normalize(df_banks["data"])
            if "score" in df_banks.columns:
                df_expanded["score"] = df_banks["score"]
            df_banks = df_expanded
            logger.info(f"Expanded columns: {list(df_banks.columns)}")

        df_banks.columns = df_banks.columns.str.upper()
        df_banks["COLLECTION_DATE"] = datetime.now()

        numeric_cols = ["CERT", "ASSET", "DEP", "DEPDOM", "ROA", "ROE", "NETINC", "OFFICES"]
        for col in numeric_cols:
            if col in df_banks.columns:
                df_banks[col] = pd.to_numeric(df_banks[col], errors="coerce")

        output_file = self.output_dir / "active_banks.csv"
        df_banks.to_csv(output_file, index=False)
        logger.info(f"✓ Saved {len(df_banks)} active banks to {output_file}")

        return df_banks

    def collect_bank_financials_timeseries(self, cert_list, start_date="2020-01-01"):
        """Collect quarterly financial data for banks"""
        logger.info("Starting quarterly financials collection...")
        logger.info(f"Processing {len(cert_list)} banks...")

        all_financials = []
        quarters = self._generate_quarters(start_date)

        for i, cert in enumerate(cert_list):
            if pd.isna(cert):
                continue

            try:
                cert = int(cert)
            except (ValueError, TypeError):
                logger.warning(f"Invalid CERT value: {cert}")
                continue

            for quarter_date in quarters:
                quarter_str = quarter_date.strftime("%Y%m%d")

                url = f"{self.base_url}/financials"
                params = {
                    "filters": f"CERT:{cert} AND REPDTE:{quarter_str}",
                    "fields": "CERT,REPDTE,ASSET,LIAB,DEP,DEPDOM,EQTOT,NETINC,ROA,ROE,LNRE,LNCI",
                    "format": "json",
                    "limit": 10,
                }

                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()

                    if "data" in data and len(data["data"]) > 0:
                        for record in data["data"]:
                            extracted_data = self._extract_record_data(record)
                            if extracted_data:
                                normalized = {k.upper(): v for k, v in extracted_data.items()}
                                all_financials.append(normalized)

                except requests.exceptions.RequestException:
                    continue
                except Exception as e:
                    logger.debug(f"Error for CERT {cert}, quarter {quarter_str}: {e}")
                    continue

                time.sleep(0.2)

            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1}/{len(cert_list)} banks, collected {len(all_financials)} records")
                if len(all_financials) > 0:
                    df_temp = pd.DataFrame(all_financials)
                    temp_file = self.output_dir / "bank_financials_temp.csv"
                    df_temp.to_csv(temp_file, index=False)

        if len(all_financials) == 0:
            logger.warning("No financial data collected!")
            df_financials = pd.DataFrame(columns=["CERT", "REPDTE", "ASSET", "LIAB", "DEP"])
        else:
            df_financials = pd.DataFrame(all_financials)
            numeric_cols = [
                "CERT", "ASSET", "LIAB", "DEP", "DEPDOM", "EQTOT",
                "NETINC", "ROA", "ROE", "LNRE", "LNCI"
            ]
            for col in numeric_cols:
                if col in df_financials.columns:
                    df_financials[col] = pd.to_numeric(df_financials[col], errors="coerce")

        output_file = self.output_dir / "bank_financials_timeseries.csv"
        df_financials.to_csv(output_file, index=False)
        logger.info(f"✓ Saved {len(df_financials)} financial records to {output_file}")

        return df_financials

    def _generate_quarters(self, start_date):
        start = pd.to_datetime(start_date)
        end = datetime.now()
        quarters = []
        current = start

        while current <= end:
            quarter_end = pd.Period(current, freq="Q").end_time
            quarters.append(quarter_end)
            current = quarter_end + timedelta(days=1)

        return quarters

    def collect_failed_banks(self):
        """Collect historical failed banks data"""
        logger.info("Starting failed banks collection...")

        url = f"{self.base_url}/failures"
        params = {
            "filters": 'FAILDATE:["2000-01-01" TO "2024-12-31"]',
            "fields": "NAME,CERT,FAILDATE,QBFDEP,COST,CITY,STNAME,RESTYPE1",
            "sort_by": "FAILDATE",
            "sort_order": "DESC",
            "limit": 10000,
            "format": "json",
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "data" in data and len(data["data"]) > 0:
                failed_banks_data = []
                for record in data["data"]:
                    extracted = self._extract_record_data(record)
                    if extracted:
                        failed_banks_data.append(extracted)

                if len(failed_banks_data) == 0:
                    logger.warning("No failed bank records extracted")
                    return pd.DataFrame()

                df_failed = pd.DataFrame(failed_banks_data)
                df_failed.columns = df_failed.columns.str.upper()

                if "CERT" in df_failed.columns:
                    df_failed["CERT"] = pd.to_numeric(df_failed["CERT"], errors="coerce")
                if "QBFDEP" in df_failed.columns:
                    df_failed["QBFDEP"] = pd.to_numeric(df_failed["QBFDEP"], errors="coerce")
                if "COST" in df_failed.columns:
                    df_failed["COST"] = pd.to_numeric(df_failed["COST"], errors="coerce")

                output_file = self.output_dir / "failed_banks.csv"
                df_failed.to_csv(output_file, index=False)
                logger.info(f"✓ Saved {len(df_failed)} failed banks to {output_file}")

                return df_failed
            else:
                logger.warning("No failed bank data in API response")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error collecting failed banks: {e}")
            return pd.DataFrame()

    def generate_synthetic_lending_relationships(self, df_banks, density=0.05):
        """Generate synthetic interbank lending relationships"""
        logger.info("Generating synthetic lending relationships...")

        required_cols = ["CERT", "ASSET", "NAME"]
        missing_cols = [col for col in required_cols if col not in df_banks.columns]

        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            logger.info(f"Available columns: {list(df_banks.columns)}")
            return pd.DataFrame()

        df_banks_clean = df_banks.dropna(subset=["CERT", "ASSET"]).copy()

        if len(df_banks_clean) == 0:
            logger.error("No valid banks after cleaning")
            return pd.DataFrame()

        n_banks = min(1000, len(df_banks_clean))
        df_sorted = df_banks_clean.nlargest(n_banks, "ASSET").reset_index(drop=True)

        logger.info(f"Generating relationships for {len(df_sorted)} banks...")

        relationships = []

        for idx in range(len(df_sorted)):
            lender = df_sorted.iloc[idx]

            base_connections = int(np.log10(float(lender["ASSET"]) + 1))
            num_borrowers = max(1, np.random.poisson(base_connections * density * 10))
            num_borrowers = min(num_borrowers, len(df_sorted) - 1)

            if num_borrowers == 0:
                continue

            asset_values = df_sorted["ASSET"].values.astype(float)
            weights = 1 / (asset_values + 1)
            weights = weights / weights.sum()

            try:
                borrower_indices = np.random.choice(len(df_sorted), size=num_borrowers, replace=False, p=weights)
            except Exception:
                borrower_indices = np.random.choice(len(df_sorted), size=num_borrowers, replace=False)

            for borrower_idx in borrower_indices:
                if borrower_idx == idx:
                    continue

                borrower = df_sorted.iloc[borrower_idx]
                lending_pct = np.random.uniform(0.01, 0.10)
                lending_amount = float(borrower["ASSET"]) * lending_pct

                relationships.append({
                    "lender_cert": int(lender["CERT"]),
                    "lender_name": str(lender["NAME"])[:100],
                    "borrower_cert": int(borrower["CERT"]),
                    "borrower_name": str(borrower["NAME"])[:100],
                    "lending_amount": lending_amount,
                    "relationship_type": "interbank_loan",
                    "start_date": datetime.now() - timedelta(days=int(np.random.randint(30, 1095))),
                    "is_synthetic": True,
                })

            if (idx + 1) % 100 == 0:
                logger.info(f"Processed {idx + 1}/{len(df_sorted)} lenders...")

        if len(relationships) == 0:
            logger.warning("No relationships generated")
            return pd.DataFrame()

        df_lending = pd.DataFrame(relationships)
        output_file = self.output_dir / "interbank_lending_relationships.csv"
        df_lending.to_csv(output_file, index=False)
        logger.info(f"✓ Generated {len(df_lending)} lending relationships")

        return df_lending


# ----------------- Streaming helpers -----------------

def stream_fdic():
    logger.info("🔌 FDIC streaming server starting (file-drop mode)...")

    collector = FDICDataCollector()

    df_banks = collector.collect_active_banks()
    logger.info(f"🏦 Active banks collected: {len(df_banks)}")
    out_path = OUTPUT_DIR / "fdic_active_banks.json"
    df_banks.to_json(out_path, orient="records")
    logger.info(f"📄 Saved → {out_path}")

    cert_list = df_banks["CERT"].astype(str).tolist()[:50]
    df_financials = collector.collect_bank_financials_timeseries(cert_list)
    logger.info(f"📊 Financial timeseries collected: {len(df_financials)} rows")
    out_path = OUTPUT_DIR / "fdic_financial_timeseries.json"
    df_financials.to_json(out_path, orient="records")
    logger.info(f"📄 Saved → {out_path}")

    df_failed = collector.collect_failed_banks()
    logger.info(f"💀 Failed banks collected: {len(df_failed)}")
    out_path = OUTPUT_DIR / "fdic_failed_banks.json"
    df_failed.to_json(out_path, orient="records")
    logger.info(f"📄 Saved → {out_path}")

    df_edges = collector.generate_synthetic_lending_relationships(df_banks, density=0.05)
    logger.info(f"🔗 Interbank synthetic edges: {len(df_edges)}")
    out_path = OUTPUT_DIR / "fdic_interbank_edges.json"
    df_edges.to_json(out_path, orient="records")
    logger.info(f"📄 Saved → {out_path}")

    logger.info("✅ FDIC streaming complete (file-drop mode).")


def main():
    parser = argparse.ArgumentParser(description="FDIC collector/streaming (file-drop).")
    parser.add_argument("--mode", choices=["stream", "collect"], default="stream", help="stream JSON to file-drop or collect full datasets to disk")
    parser.add_argument("--output-dir", default="data/banking", help="Output directory for collected CSVs (collect mode)")
    parser.add_argument("--sample-cert-count", type=int, default=200, help="Number of largest banks to collect financials for (collect mode)")
    args = parser.parse_args()

    if args.mode == "collect":
        collector = FDICDataCollector(output_dir=args.output_dir)
        print("=" * 80)
        print("PART 1: BANKING NETWORK DATA COLLECTION")
        print("=" * 80)

        print("\n[Step 1/4] Collecting active banks...")
        df_banks = collector.collect_active_banks()
        if df_banks.empty:
            print("❌ ERROR: Failed to collect bank data")
            return

        df_valid = df_banks.dropna(subset=["CERT", "ASSET"])
        cert_list = df_valid.nlargest(args.sample_cert_count, "ASSET")["CERT"].tolist()

        print("\n[Step 2/4] Collecting quarterly financial data...")
        df_financials = collector.collect_bank_financials_timeseries(cert_list)

        print("\n[Step 3/4] Collecting failed banks...")
        df_failed = collector.collect_failed_banks()

        print("\n[Step 4/4] Generating synthetic lending relationships...")
        df_lending = collector.generate_synthetic_lending_relationships(df_banks)

        total_rows = len(df_banks) + len(df_financials) + len(df_failed) + len(df_lending)
        print("\n" + "=" * 80)
        print("PART 1 COMPLETE ✓")
        print("=" * 80)
        print(f"Total rows collected: {total_rows:,}")
        print(f"\nBreakdown:")
        print(f"  • Active banks:          {len(df_banks):>6,} rows")
        print(f"  • Bank financials:       {len(df_financials):>6,} rows")
        print(f"  • Failed banks:          {len(df_failed):>6,} rows")
        print(f"  • Lending relationships: {len(df_lending):>6,} rows")
        print(f"\n📁 Files saved to: {args.output_dir}")
        print("=" * 80)
    else:
        stream_fdic()


if __name__ == "__main__":
    main()
