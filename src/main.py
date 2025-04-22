# src/main.py

import os
from datetime import datetime, timedelta

from treasuryData.pipeline import merge_yield_series_incremental
import bigQueryUtils
from treasuryData.config import (
    SERIES_IDS, SPREADS, GOOGLE_CLOUD_PROJECT,
    BIGQUERY_DATASET, FRED_API_KEY
)

# Set up Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "credentials.json")
)

# BigQuery table configs
TABLE_NAME = "daily_treasury_yields"
CRYPTO_TABLE = "crypto_prices"
LOG_TABLE = "audit_log"


# --- Macro Data Logic ---
def run_macro_pipeline():
    latest_date = bigQueryUtils.get_latest_date_from_bq(
        GOOGLE_CLOUD_PROJECT, BIGQUERY_DATASET, TABLE_NAME
    )
    start_date = (latest_date + timedelta(days=1)).date() if latest_date else datetime.strptime("1968-01-01", "%Y-%m-%d").date()
    end_date = datetime.today().date()

    if start_date > end_date:
        print("✅ Macro data up to date.")
        status, row_count, error_msg = "skipped", 0, None
    else:
        print(f"📡 Fetching macro data from {start_date} to {end_date}...")
        df = merge_yield_series_incremental(
            SERIES_IDS,
            FRED_API_KEY,
            str(start_date),
            str(end_date),
            spreads_to_compute=SPREADS
        )
        bigQueryUtils.upload_to_bigquery(
            df, BIGQUERY_DATASET, TABLE_NAME, GOOGLE_CLOUD_PROJECT, mode="append"
        )
        print("✅ Macro data upload complete.")
        status, row_count, error_msg = "success", len(df), None

    bigQueryUtils.log_load_metadata(
        GOOGLE_CLOUD_PROJECT,
        BIGQUERY_DATASET,
        LOG_TABLE,
        start_date,
        end_date,
        row_count,
        status,
        error_msg
    )





# --- CLI Runner ---
if __name__ == "__main__":
    import sys
    from flowAnalysis.fiat_tracker import interactive_fiat_tracker, list_fiat_stable_pairs

    print("\n📊 Choose a pipeline to run:")
    print("1. Update Macro Treasury Yield Data")
    print("2. Run Stablecoin --> Exchanges (Custom Period)")
    print("3. Track Fiat → Stablecoin Market Trades (via CEX)")
    print("4. View Available Fiat ⇄ Stablecoin Pairs")
    print("5. Exit")

    choice = input("\nEnter number: ").strip()

    if choice == "1":
        run_macro_pipeline()

    elif choice == "2":
        from flowAnalysis.flowfetcher import interactive_stablecoin_flow_tracker
        interactive_stablecoin_flow_tracker()

    elif choice == "3":
        interactive_fiat_tracker()

    elif choice == "4":
        exch = input("🌐 Exchange name (e.g. binance, kraken, coinbase): ").strip()
        fiat = input("💱 (Optional) Fiat hint (e.g. USD, EUR) or press Enter to skip: ").strip()
        stable = input("🪙 (Optional) Stablecoin hint (e.g. USDT, USDC) or press Enter to skip: ").strip()
        list_fiat_stable_pairs(exch, fiat or None, stable or None)

    elif choice == "5":
        print("👋 Exiting.")
        sys.exit()
    else:
        print("❌ Invalid choice. Please try again.")
