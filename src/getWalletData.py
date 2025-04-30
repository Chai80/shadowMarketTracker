from shroomdk import ShroomDK
import pandas as pd
import time
from datetime import timedelta
import os
from google.cloud import bigquery

# Initialize Flipside + BigQuery
sdk = ShroomDK("fbd8fab9-d866-48aa-a895-ce9fe5ecaaa5")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "credentials.json")
)
client = bigquery.Client()

# BigQuery config
PROJECT = "macropipeline"
DATASET = "KYCWallets"
FINAL_TABLE = "CEXWallets"
STAGING_TABLE = "CEXWallets_staging"
FINAL_TABLE_ID = f"{PROJECT}.{DATASET}.{FINAL_TABLE}"
STAGING_TABLE_ID = f"{PROJECT}.{DATASET}.{STAGING_TABLE}"

# Batch config
BATCH_SIZE = 100_000
OFFSET = 0
MAX_ROWS = 7_000_000

# SQL query: Binance active in 2022–2023 only
QUERY_TEMPLATE = """
WITH active_labeled AS (
  SELECT
    LOWER(l.address) AS address,
    MIN(LOWER(l.address_name)) AS entity,
    'CEX' AS label,
    ROW_NUMBER() OVER (ORDER BY LOWER(l.address)) AS rownum
  FROM ethereum.core.dim_labels l
  JOIN ethereum.core.fact_transactions tx
    ON LOWER(tx.to_address) = LOWER(l.address)
  WHERE (
    LOWER(l.address_name) = 'binance deposit_wallet' OR
    LOWER(l.address_name) = 'binance hot_wallet' OR
    LOWER(l.address_name) = 'binance us deposit funder 1' OR
    LOWER(l.address_name) LIKE 'binance %' OR
    LOWER(l.address_name) LIKE 'binance-%'
  )
  AND tx.block_timestamp BETWEEN '2022-01-01' AND '2024-12-31'
  GROUP BY l.address
)
SELECT address, entity, label
FROM active_labeled
WHERE rownum > {offset} AND rownum <= {offset} + {limit}
"""

# Ingestion loop
print("🚀 Starting Binance timeline-filtered upload (2022–2023)...\n")
start_time = time.time()
batches_completed = 0
total_uploaded = 0

while OFFSET < MAX_ROWS:
    sql = QUERY_TEMPLATE.format(limit=BATCH_SIZE, offset=OFFSET)

    try:
        response = sdk.query(sql)
        df = pd.DataFrame(response.records)
    except Exception as e:
        print(f"❌ Query failed at offset {OFFSET}: {e}")
        break

    if df.empty:
        print(f"✅ All data fetched. No more results after offset {OFFSET}.")
        break

    df = df[["address", "entity", "label"]]
    print(f"🧮 Batch {batches_completed}: fetched {len(df):,} rows")

    try:
        # Upload to staging
        job = client.load_table_from_dataframe(df, STAGING_TABLE_ID)
        job.result()
        print(f"📥 Uploaded to staging table ({len(df):,} rows)")

        # Deduplicated insert into final table
        insert_sql = f"""
        INSERT INTO `{FINAL_TABLE_ID}` (address, entity, label)
        SELECT address, entity, label
        FROM `{STAGING_TABLE_ID}`
        WHERE LOWER(address) NOT IN (
          SELECT LOWER(address) FROM `{FINAL_TABLE_ID}`
        )
        """
        client.query(insert_sql).result()
        print("✅ Deduplicated insert completed.")

        total_uploaded += len(df)

    except Exception as err:
        print(f"❌ Upload or insert failed at offset {OFFSET}: {err}")
        break

    batches_completed += 1
    OFFSET += BATCH_SIZE

# Summary
elapsed = timedelta(seconds=int(time.time() - start_time))
print(f"\n🎉 Done! Uploaded {total_uploaded:,} timeline-filtered Binance rows across {batches_completed} batches in {elapsed}.")
