import requests

def fetch_yield_data(series_id, api_key, start_date, end_date):
    """
    Fetch raw FRED data for a given series_id between start_date and end_date.
    """
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return response.json()['observations']

#Not used yet, debating if we even need rrp_data
def fetch_rrp_data(api_key):
    data = fetch_yield_data("RRPONTSYD", api_key, "2013-10-14", "2025-04-08")
    return [
        obs for obs in data if obs["date"] >= "2013-10-14"
    ]
