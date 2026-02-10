# scripts/fetch_to_json.py
"""
Created as an alternative to using sqlite. Not sure that json files are the way forward though.
"""
import os, json, pandas as pd
from datetime import datetime, timedelta, timezone
from flask_app import STATIONS, get_station_data  # reuse your fetch logic  

OUT_DIR = os.environ.get("OUT_DIR", "docs/data")
NDAYS = int(os.environ.get("NDAYS", "7"))

os.makedirs(OUT_DIR, exist_ok=True)
now = datetime.now(timezone.utc)

for key, meta in STATIONS.items():
    resp = get_station_data(key, ndays=NDAYS)  # returns {"readings":[{"dateTime","value"},...]}  
    rows = resp.get("readings", [])
    # normalize to ts_utc & value for the browser
    norm = []
    for r in rows:
        # robust parse -> epoch seconds
        dt = pd.to_datetime(r["dateTime"], utc=True, errors="coerce")
        if pd.isna(dt): 
            continue
        norm.append({"ts_utc": int(dt.timestamp()), "value": float(r["value"])})
    norm.sort(key=lambda d: d["ts_utc"])

    out = {
        "station_key": key,
        "station_name": meta.get("name", key),
        "source": meta.get("source", ""),
        "generated_at": now.isoformat(),
        "data": norm
    }
    with open(os.path.join(OUT_DIR, f"{key}.json"), "w") as f:
        json.dump(out, f)
