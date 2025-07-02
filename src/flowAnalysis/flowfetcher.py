from datetime import datetime, timedelta
import os
import pandas as pd
from dotenv import load_dotenv
from bigQueryUtils import upload_flipside_to_bq
import snowflake.connector

# ──────────────────────────────────────────────────────────────
# 🔐 Load .env variables
# ──────────────────────────────────────────────────────────────
load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "COMPUTE_WH",
    "database": "ETHEREUM_ONCHAIN_CORE_DATA",
    "schema": "CORE",
}


# ──────────────────────────────────────────────────────────────
# 🔄 Core Query
# ──────────────────────────────────────────────────────────────
def fetch_usdt_transfers_snowflake(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = ctx.cursor()

    sql = f"""
    SELECT
        t.block_timestamp AS date,
        t.tx_hash,
        t.from_address,
        l_from.label AS from_entity,
        t.to_address,
        l_to.label   AS to_entity,
        t.amount     AS usdt_amount
    FROM CORE.ez_token_transfers t
    LEFT JOIN CORE.DIM_LABELS l_from
           ON LOWER(t.from_address) = LOWER(l_from.address)
    LEFT JOIN CORE.DIM_LABELS l_to
           ON LOWER(t.to_address)  = LOWER(l_to.address)
    WHERE t.symbol = 'USDT'
      AND t.block_timestamp BETWEEN '{start_dt:%Y-%m-%d %H:%M:%S}'
                                AND '{end_dt:%Y-%m-%d %H:%M:%S}'
    ORDER BY t.block_timestamp ASC
"""


    cs.execute(sql)
    df = cs.fetch_pandas_all()
    ctx.close()
    return df


# ──────────────────────────────────────────────────────────────
# 🕓 Hourly & Daily Pulls
# ──────────────────────────────────────────────────────────────
def fetch_usdt_flows_hourly_chunks(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    all_dfs = []
    while start_dt < end_dt:
        next_dt = min(start_dt + timedelta(hours=1), end_dt)
        df = fetch_usdt_transfers_snowflake(start_dt, next_dt)
        if not df.empty:
            all_dfs.append(df)
        start_dt = next_dt
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


def fetch_usdt_flows_daily_range(start_dt: datetime, end_dt: datetime, export_dir: str):
    os.makedirs(export_dir, exist_ok=True)
    daily_outputs = []
    while start_dt < end_dt:
        next_dt = min(start_dt + timedelta(days=1), end_dt)
        df = fetch_usdt_flows_hourly_chunks(start_dt, next_dt)
        if not df.empty:
            fname = f"usdtflows_{start_dt.strftime('%Y-%m-%d')}.csv"
            fpath = os.path.join(export_dir, fname)
            df.to_csv(fpath, index=False)
            print(f"✅ Saved: {fpath} ({len(df)} rows)")
            daily_outputs.append((fpath, df))
        start_dt = next_dt
    return daily_outputs


# ──────────────────────────────────────────────────────────────
# 🧪 CLI Interface
# ──────────────────────────────────────────────────────────────
def interactive_stablecoin_flow_tracker():
    print("\n🔗 USDT On-Chain Flow Tracker (Snowflake)")

    while True:
        try:
            start_dt = datetime.strptime(input("🕐 Start datetime (YYYY-MM-DD HH:MM): "), "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(input("🕐 End datetime (YYYY-MM-DD HH:MM): "), "%Y-%m-%d %H:%M")
            break
        except ValueError:
            print("❌ Invalid format. Try again.")

    tag = input("🏷️ Label for this pull: ").strip().replace(" ", "_")
    table_id = input("📄 BigQuery table (default: usdt_to_binance): ").strip() or "usdt_to_binance"
    folder = os.path.join("SNOWFLAKE_USDT_FLOWS", tag)
    os.makedirs(folder, exist_ok=True)

    daily = input("🔁 Chunk into daily batches? (y/n): ").strip().lower() == "y"
    upload = input("🚀 Upload to BigQuery? (y/n): ").strip().lower() == "y"

    if daily:
        results = fetch_usdt_flows_daily_range(start_dt, end_dt, export_dir=folder)
    else:
        df = fetch_usdt_transfers_snowflake(start_dt, end_dt)
        fpath = os.path.join(folder, f"usdtflows_{start_dt:%Y%m%d_%H%M}_to_{end_dt:%Y%m%d_%H%M}.csv")
        df.to_csv(fpath, index=False)
        print(f"✅ Saved: {fpath} ({len(df)} rows)")
        results = [(fpath, df)]

    if upload:
        for _, df in results:
            df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
            df["date_UTC"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df["tx_hash"] = df["tx_hash"].astype(str)
            df["from_address"] = df["from_address"].astype(str)
            df["to_address"] = df["to_address"].astype(str)
            df["usdt_amount"] = pd.to_numeric(df["usdt_amount"], errors="coerce")
            df["label"] = tag
            print("🔍 Uploading sample row:", df.head(1).to_dict())

            upload_flipside_to_bq(
                df=df,
                dataset_id="usdtFlows",
                table_id=table_id,
                tag=tag,
            )
        print("✅ Upload complete.")
