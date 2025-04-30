# src/bigquery_utils.py
from google.cloud import bigquery
from pandas.api.types import is_datetime64_any_dtype



def get_latest_date_from_bq(project_id, dataset, table):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT MAX(date) as latest_date
    FROM `{project_id}.{dataset}.{table}`
    """
    result = client.query(query).result()
    rows = list(result)
    return rows[0].latest_date if rows else None

def upload_to_bigquery(df, dataset, table, project_id, mode="append"):
    from pandas_gbq import to_gbq

    to_gbq(
        df,
        destination_table=f"{dataset}.{table}",
        project_id=project_id,
        if_exists=mode
    )

import pandas as pd
from datetime import datetime
from pandas_gbq import to_gbq

def log_load_metadata(project_id, dataset_id, table_name, start_date, end_date, row_count, status, error_msg):
    log_df = pd.DataFrame([{
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_name": table_name,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "row_count": row_count,
        "status": status,
        "error_message": error_msg,
        "load_time": datetime.utcnow()
    }])

    # Destination table in the form dataset.table
    full_table_id = f"{dataset_id}.load_audit_log"

    # This will auto-create the table if it doesn't exist
    to_gbq(
        dataframe=log_df,
        destination_table=full_table_id,
        project_id=project_id,
        if_exists="append"
    )

    print("📝 Audit log entry inserted.")



def upload_fiat_trades_to_bq(df, project_id, dataset_id, table_id, tag):
    #  Rename timestamp to match BigQuery schema if not already done
    #  not sure why i keep getting this problem but this fixes it for now revisit anotehr time
    if "timestamp" in df.columns and "date" not in df.columns:
        df["date"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df.drop(columns=["timestamp"], inplace=True)

    elif "datetime" in df.columns and "date" not in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        df.drop(columns=["datetime"], inplace=True)

    #  Drop any leftover unwanted columns
    for col in ["timestamp", "datetime", "Date_UTC_Time"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    df['label'] = tag  # Add a tag for traceability

    print("📋 Final column types before BigQuery upload:")
    print(df.dtypes)
    print("📋 Final columns:", df.columns.tolist())

    schema = [
    bigquery.SchemaField("date", "TIMESTAMP"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "FLOAT"),
    bigquery.SchemaField("vwap", "FLOAT"),
    bigquery.SchemaField("exchange", "STRING"),
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("adjusted_pair", "STRING"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField("usdt_volume", "FLOAT"),  #  NEW
    bigquery.SchemaField("fiat_volume", "FLOAT"),  #  NEW
    ]

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(schema=schema)

    client = bigquery.Client(project=project_id)
    print(f"🚀 Uploading to BigQuery: {table_ref}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print("✅ Upload complete.")

import time
from google.api_core.exceptions import ServiceUnavailable, GoogleAPICallError


from google.cloud import bigquery
from pandas.api.types import is_datetime64_any_dtype
import time
from google.api_core.exceptions import ServiceUnavailable, GoogleAPICallError

def upload_flipside_to_bq(df, dataset_id, table_id, tag, max_retries=3):
    client = bigquery.Client()
    project_id = client.project
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Ensure datetime type for upload
    if "date" in df.columns and not is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    # Drop bad columns
    if "__row_index" in df.columns:
        df.drop(columns=["__row_index"], inplace=True)

    # Final preview
    print("📋 Final df.dtypes:\n", df.dtypes)
    print("📋 Columns:", df.columns.tolist())
    print("📋 Preview:\n", df.head(2).to_dict())

    schema = [
        bigquery.SchemaField("date_UTC", "STRING"),
        bigquery.SchemaField("tx_hash", "STRING"),
        bigquery.SchemaField("from_address", "STRING"),
        bigquery.SchemaField("from_entity", "STRING"),
        bigquery.SchemaField("to_address", "STRING"),
        bigquery.SchemaField("to_entity", "STRING"),
        bigquery.SchemaField("usdt_amount", "FLOAT"),
        bigquery.SchemaField("label", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(schema=schema)

    for attempt in range(max_retries):
        try:
            print(f"🚀 Uploading to BigQuery: {table_ref} (Attempt {attempt + 1})")
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result(timeout=60)
            print("✅ Upload complete.")
            return
        except ServiceUnavailable:
            print(f"⚠️ Upload failed (503). Retrying in {2 ** attempt} seconds...")
            time.sleep(2 ** attempt)
        except GoogleAPICallError as e:
            print(f"❌ Upload failed (API error): {e}")
            return
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return
