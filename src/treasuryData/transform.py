import numpy as np
import pandas as pd

def clean_yield_data(raw_observations, series_id):
    if not raw_observations:
        print(f"⚠️ Warning: No data returned for series {series_id}")
        return pd.DataFrame()

    df = pd.DataFrame(raw_observations)

    # Ensure required columns exist
    if 'date' not in df.columns or 'value' not in df.columns:
        print(f"⚠️ Warning: Missing 'date' or 'value' in data for {series_id}")
        return pd.DataFrame()

    df = df[['date', 'value']].copy()
    df = df.rename(columns={'value': series_id})
    df['date'] = pd.to_datetime(df['date'])
    df[series_id] = pd.to_numeric(df[series_id].replace('.', np.nan), errors='coerce')

    df = df.sort_values('date')
    df[series_id] = df[series_id].ffill()

    return df



# src/transform.py

def calculate_spreads(df, spreads_to_compute):
    for long, short in spreads_to_compute:
        if long not in df.columns or short not in df.columns:
            print(f"⚠️ Skipping spread {long} - {short}: missing column(s) in dataframe.")
            continue

        spread_name = f"{long}_{short}_spread"
        df[spread_name] = df[long] - df[short]

    return df

