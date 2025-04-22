import os
import json

# Get the directory of this file
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(CURRENT_DIR, "../..", "config.json")  # goes one level up

# Load config.json
with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

# Extract values
SERIES_IDS = CONFIG.get("series_ids", [])
SPREADS = [tuple(pair) for pair in CONFIG.get("spreads", [])]
OBSERVATION_START = CONFIG.get("dateRange", {}).get("observationStart", "1968-01-01")
OBSERVATION_END = CONFIG.get("dateRange", {}).get("observationEnd", "2025-04-08")

# Optional: hardcoded cloud values
GOOGLE_CLOUD_PROJECT = "macropipeline"
BIGQUERY_DATASET = "macroDataset"
FRED_API_KEY = "c38ea813ce3396dd6174827d8dba691a"
