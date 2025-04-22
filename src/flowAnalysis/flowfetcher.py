from datetime import datetime, timedelta
import os
import pandas as pd
from flipside import Flipside
from bigQueryUtils import upload_flipside_to_bq


FLIPSIDE_API_KEY = "fbd8fab9-d866-48aa-a895-ce9fe5ecaaa5"

def fetch_USDT_transactionsFlipside(start_dt: datetime, end_dt: datetime, sdk=None):
    if sdk is None:
        sdk = Flipside(FLIPSIDE_API_KEY)

    query = f"""
    SELECT
      t.block_timestamp AS date,
      t.tx_hash,
      t.from_address,
      l_from.label AS from_entity,
      t.to_address,
      l_to.label AS to_entity,
      t.amount AS usdt_amount
    FROM ethereum.core.ez_token_transfers t
    LEFT JOIN ethereum.core.dim_labels l_from ON LOWER(t.from_address) = LOWER(l_from.address)
    LEFT JOIN ethereum.core.dim_labels l_to ON LOWER(t.to_address) = LOWER(l_to.address)
    WHERE
      t.symbol = 'USDT'
      AND t.block_timestamp BETWEEN TIMESTAMP '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'
                              AND TIMESTAMP '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'
    ORDER BY t.block_timestamp ASC
    """

    print(f"\n🔍 Querying Flipside for USDT flows from {start_dt} to {end_dt}...")
    results = sdk.query(query)
    return pd.DataFrame(results.records)


def fetch_usdt_flows_hourly_chunks(start_dt: datetime, end_dt: datetime, sdk=None):
    current_start = start_dt
    all_dfs = []

    while current_start < end_dt:
        current_end = min(current_start + timedelta(hours=1), end_dt)
        df = fetch_USDT_transactionsFlipside(current_start, current_end, sdk=sdk)
        if not df.empty:
            all_dfs.append(df)
        current_start = current_end

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def fetch_usdt_flows_daily_range(start_dt: datetime, end_dt: datetime, export_dir: str, sdk=None):
    os.makedirs(export_dir, exist_ok=True)
    daily_outputs = []
    current_start = start_dt

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=1), end_dt)
        df = fetch_usdt_flows_hourly_chunks(current_start, current_end, sdk=sdk)

        if not df.empty:
            file_name = f"usdtflows_{current_start.strftime('%Y-%m-%d')}.csv"
            file_path = os.path.join(export_dir, file_name)
            df.to_csv(file_path, index=False)
            print(f"✅ Saved full-day file: {file_path} ({len(df)} rows)")
            daily_outputs.append((file_path, df))

        current_start = current_end

    return daily_outputs


from datetime import datetime, timedelta
import os
import pandas as pd
from flipside import Flipside
from bigQueryUtils import upload_flipside_to_bq
from flowAnalysis.flowfetcher import fetch_usdt_flows_daily_range, fetch_USDT_transactionsFlipside
from pandas.api.types import is_datetime64_any_dtype

def interactive_stablecoin_flow_tracker():
    print("\n🔗 USDT On-Chain Flow Tracker (Flipside API)")

    while True:
        try:
            start_dt = datetime.strptime(input("🕐 Start datetime (YYYY-MM-DD HH:MM): ").strip(), "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(input("🕐 End datetime (YYYY-MM-DD HH:MM): ").strip(), "%Y-%m-%d %H:%M")
            break
        except ValueError:
            print("❌ Invalid format. Please use YYYY-MM-DD HH:MM")

    tag = input("🏷️ Enter a label for this time period (e.g. russian_sanctions): ").strip().replace(" ", "_")
    table_id = input("📄 BigQuery table to upload to (default: usdt_to_binance): ").strip() or "usdt_to_binance"

    folder = os.path.join("FLIPSIDE_USDT_FLOWS", tag)
    os.makedirs(folder, exist_ok=True)

    sdk = Flipside(FLIPSIDE_API_KEY)

    daily = input("🔁 Chunk into daily batches? (y/n): ").strip().lower()
    if daily == "y":
        upload_flag = input("\n🚀 Upload each daily file to BigQuery after saving? (y/n): ").strip().lower() == "y"
        results = fetch_usdt_flows_daily_range(start_dt, end_dt, export_dir=folder, sdk=sdk)

        if upload_flag:
            for file_path, df in results:
                df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
                df["date_UTC"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df["tx_hash"] = df["tx_hash"].astype(str)
                df["from_address"] = df["from_address"].astype(str)
                df["from_entity"] = df["from_entity"].astype(str)
                df["to_address"] = df["to_address"].astype(str)
                df["to_entity"] = df["to_entity"].astype(str)
                df["usdt_amount"] = pd.to_numeric(df["usdt_amount"], errors="coerce")
                df["label"] = tag

                print("🔍 DataFrame dtypes before upload:")
                print(df.dtypes)
                print("🔍 Nulls per column:")
                print(df.isnull().sum())
                print("🔍 First row preview:")
                print(df.head(1).to_dict())

                upload_flipside_to_bq(
                    df=df,
                    dataset_id="usdtFlows",
                    table_id=table_id,
                    tag=tag
                )
        print("✅ All daily chunks saved.")

    else:
        df = fetch_USDT_transactionsFlipside(start_dt, end_dt, sdk=sdk)
        if df is None or df.empty:
            print("⚠️ No data returned.")
            return

        output_path = os.path.join(folder, f"usdtflows_{start_dt.strftime('%Y%m%d_%H%M')}_to_{end_dt.strftime('%Y%m%d_%H%M')}.csv")
        df.to_csv(output_path, index=False)
        print(f"✅ Saved to {output_path}")

        upload = input("\n🚀 Upload to BigQuery? (y/n): ").strip().lower()
        if upload == "y":
            df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
            df["date (UTC)"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

            df["tx_hash"] = df["tx_hash"].astype(str)
            df["from_address"] = df["from_address"].astype(str)
            df["from_entity"] = df["from_entity"].astype(str)
            df["to_address"] = df["to_address"].astype(str)
            df["to_entity"] = df["to_entity"].astype(str)
            df["usdt_amount"] = pd.to_numeric(df["usdt_amount"], errors="coerce")
            df["label"] = tag

            print("🔍 DataFrame dtypes before upload:")
            print(df.dtypes)
            print("🔍 Nulls per column:")
            print(df.isnull().sum())
            print("🔍 First row preview:")
            print(df.head(1).to_dict())

            upload_flipside_to_bq(
                df=df,
                dataset_id="usdtFlows",
                table_id=table_id,
                tag=tag
            )
