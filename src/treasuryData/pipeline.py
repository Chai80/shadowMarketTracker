# src/pipeline.py

def merge_yield_series_incremental(series_ids, api_key, start_date, end_date, spreads_to_compute=None):
    import pandas as pd
    import gc
    from treasuryData.fetch import fetch_yield_data
    from treasuryData.transform import clean_yield_data, calculate_spreads


    merged_df = None
    missing_series = []

    for sid in series_ids:
        raw = fetch_yield_data(sid, api_key, start_date, end_date)
        df = clean_yield_data(raw, sid)

        if df.empty:
            print(f"⚠️ Skipping merge for {sid} — no data returned.")
            missing_series.append(sid)
            continue

        if merged_df is None:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on='date', how='outer')

        del df, raw
        gc.collect()

    if merged_df is None or merged_df.empty:
        print("❌ No data could be merged from any series.")
        return pd.DataFrame()

    merged_df.sort_values(by='date', inplace=True)
    merged_df.reset_index(drop=True, inplace=True)

    if spreads_to_compute:
        merged_df = calculate_spreads(merged_df, spreads_to_compute)

    if missing_series:
        print(f"\n⚠️ The following series had no data and were skipped: {', '.join(missing_series)}")

    return merged_df
