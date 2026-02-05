"""
Flask web app for river water level monitoring.
Access at http://localhost:5000 in your browser.
"""

from flask import Flask, render_template, jsonify, request
import requests
import pandas as pd
from datetime import datetime
import numpy as np
import json
import sys
import os

# Try to import shoothill_api (optional, requires coast library)
GAUGE = None
try:
    sys.path.append(os.path.dirname(os.path.abspath("shoothill_api/shoothill_api.py")))
    try:
        from shoothill_api import GAUGE
    except ImportError:
        from shoothill_api.shoothill_api import GAUGE
except Exception as e:
    print(f"Warning: Shoothill API not available ({e}). Shoothill stations will not work.")

app = Flask(__name__, template_folder='docs')

# Station definitions
STATIONS = {
    "chester": {
        "id": "067033_TG_148",
        "name": "Chester",
        "source": "EA",
        "description": "Environment Agency monitoring station"
    },
    "liverpool": {
        "id": "E70124",
        "name": "Liverpool",
        "source": "EA",
        "description": "Environment Agency monitoring station"
    },
    "ironbridge": {
        "id": "968",
        "name": "Ironbridge",
        "source": "Shoothill",
        "description": "Shoothill monitoring station"
    },
    "farndon": {
        "id": "972",
        "name": "Farndon",
        "source": "Shoothill",
        "description": "Shoothill monitoring station"
    }
}


def fetch_station_data(station_id: str) -> dict:
    """Fetch readings from Environment Agency flood monitoring API."""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    params = {"today": ""}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching EA data: {e}")
        return {"items": []}


def fetch_shoothill_data(station_id: str) -> dict:
    """Fetch readings from Shoothill API."""
    if GAUGE is None:
        return None
    
    try:
        ndays = 1
        date_end = np.datetime64('now')
        date_start = np.datetime64(datetime.now().date().isoformat())
        #date_start = np.datetime64(datetime.utcnow().date().isoformat())
        #date_start = date_end - np.timedelta64(ndays, 'D')
        gauge = GAUGE()
        dataset = gauge.read_shoothill_to_xarray(
            station_id=station_id,
            date_start=date_start,
            date_end=date_end
        )
        return dataset
    except Exception as e:
        print(f"Error fetching Shoothill data: {e}")
        return None


def parse_shoothill_data(dataset) -> list:
    """Convert xarray dataset to readings list."""
    readings = []
    if dataset is None:
        return readings
    
    try:
        df_temp = dataset.to_dataframe().reset_index()
        value_col = None
        
        # Find water level column
        for col in df_temp.columns:
            if col.lower() in ['water_level', 'level', 'value', 'z']:
                value_col = col
                break
        
        if value_col is None:
            time_cols = ['time', 'datetime', 'date_time']
            value_col = next(
                (col for col in df_temp.columns if col.lower() not in time_cols),
                None
            )
        
        if value_col:
            # Determine time column name
            time_col = next((c for c in df_temp.columns if c.lower() in ['time', 'datetime', 'date_time']), None)
            if time_col is None:
                # Fallback: first column that looks like a datetime index name
                time_col = 'time' if 'time' in df_temp.columns else df_temp.columns[0]

            # Parse times robustly (handles fractional seconds and mixed ISO formats)
            # Let pandas infer formats; coerce errors to NaT
            times = pd.to_datetime(df_temp[time_col], utc=True, errors='coerce')

            # Build readings only for rows with valid times and numeric values
            for idx, t in enumerate(times):
                if pd.isna(t):
                    continue
                try:
                    value = float(df_temp.iloc[idx][value_col])
                except (ValueError, TypeError, KeyError):
                    continue
                if pd.notna(value):
                    readings.append({
                        "dateTime": t.isoformat(),
                        "value": float(value)
                    })
    except Exception as e:
        print(f"Error parsing Shoothill data: {e}")
    
    return readings


def get_station_data(station_key: str) -> dict:
    """Fetch and format data for a specific station."""
    if station_key not in STATIONS:
        return {"error": "Station not found"}
    
    station = STATIONS[station_key]
    
    try:
        if station["source"] == "EA":
            data = fetch_station_data(station["id"])
            readings = [
                {
                    "dateTime": item["dateTime"],
                    "value": item["value"]
                }
                for item in data.get("items", [])
            ]
        else:  # Shoothill
            dataset = fetch_shoothill_data(station["id"])
            readings = parse_shoothill_data(dataset)
        
        if not readings:
            return {
                "station": station,
                "readings": [],
                "error": "No data available"
            }
        
        df = pd.DataFrame(readings)
        # Parse datetimes robustly (handle fractional seconds and timezones)
        original_times = df["dateTime"].astype(str).copy()
        df["dateTime"] = pd.to_datetime(df["dateTime"], utc=True, errors='coerce')
        # For any entries that remain NaT, try parsing with dateutil
        if df["dateTime"].isna().any():
            try:
                from dateutil import parser as dateutil_parser
                mask = df["dateTime"].isna()
                for idx in df[mask].index:
                    s = original_times.loc[idx]
                    try:
                        parsed = dateutil_parser.parse(s)
                        # ensure timezone-aware -> convert to UTC
                        if parsed.tzinfo is not None:
                            parsed = parsed.astimezone(tz=None)
                        df.at[idx, "dateTime"] = pd.to_datetime(parsed)
                    except Exception:
                        df.at[idx, "dateTime"] = pd.NaT
            except Exception:
                # If dateutil not available, leave NaT entries as is
                pass
        df = df.sort_values("dateTime")
        
        return {
            "station": station,
            "readings": [
                {
                    "dateTime": row["dateTime"].isoformat(),
                    "value": float(row["value"])
                }
                for _, row in df.iterrows()
            ],
            "stats": {
                "min": float(df["value"].min()),
                "max": float(df["value"].max()),
                "mean": float(df["value"].mean()),
                "count": len(df)
            }
        }
    except Exception as e:
        return {
            "station": station,
            "readings": [],
            "error": str(e)
        }


@app.route("/")
def index():
    """Serve main page."""
    return render_template("index.html", stations=STATIONS)


@app.route("/api/station/<station_key>")
def api_station(station_key):
    """API endpoint for station data."""
    data = get_station_data(station_key)
    return jsonify(data)


@app.route("/api/all")
def api_all():
    """API endpoint for all stations data."""
    all_data = {}
    for station_key in STATIONS.keys():
        all_data[station_key] = get_station_data(station_key)
    return jsonify(all_data)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
