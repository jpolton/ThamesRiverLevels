"""
Flask web app for river water level monitoring.

conda activate weir_waterlevel_web_env
python flask_app.py

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
#GAUGE = None
#try:
#    sys.path.append(os.path.dirname(os.path.abspath("shoothill_api/shoothill_api.py")))
#    try:
#        from shoothill_api import GAUGE
#    except ImportError:
#        from shoothill_api.shoothill_api import GAUGE
#except Exception as e:
#    print(f"Warning: Shoothill API not available ({e}). Shoothill stations will not work.")


try:
    # Add parent directory to path to find nrw_api if running as script
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from nrw_api.nrw_api import fetch_historical_data
except Exception as e:
    print(f"Warning: NRW API not available ({e}). NRW stations will not work.")

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
        "id": "4173", #NRW:4273 Shoothill:968,
        "name": "Ironbridge",
        "source": "NRW",
        "description": "Natural Resources Wales monitoring station",
        "parameter_id": 41
        # Addtional, EA, data on IronBridge (doesn't seem to work): https://environment.data.gov.uk/flood-monitoring/id/stations/067027_TG_127/stageScale
    },
    "farndon": {
        "id": "4170", #NRW:4170 Shoothill:972,
        "name": "Farndon",
        "source": "NRW",
        "description": "Natural Resources Wales monitoring station",
        "parameter_id": 40
    },
    "queens_park": {
        "id": "SJ46_109", #EA:SJ46_109 Shoothill:10831,
        "name": "Queens Park ground water level",
        "source": "EA",
        "description": "Environment Agency monitoring station"
    }
}
# Extra EA data on Finchett's Gutter etc: https://environment.data.gov.uk/flood-monitoring/id/stations?lat=53.1899&long=-2.8853&dist=10


def fetch_station_data(station_id: str, ndays: int = 1) -> dict:
    """Fetch readings from Environment Agency flood monitoring API, for the last `ndays` days"""
    url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}/readings"
    #if ndays == 1:
    #    params = {"today": ""}
    #else:
    date_end = np.datetime64('now')
    # subtract ndays days from date_end
    date_start = date_end - np.timedelta64(int(ndays), 'D')

    # Add 1 day to end date to ensure we capture all data through the current day
    date_end_inclusive = date_end + np.timedelta64(1, 'D')

    py_dt_start = date_start.astype('datetime64[ms]').item()   # convert to Python datetime
    py_dt_end = date_end.astype('datetime64[ms]').item()
    print(py_dt_start.strftime('%Y-%m-%dT%H:%M:%SZ'))
    print(py_dt_end.strftime('%Y-%m-%dT%H:%M:%SZ'))
    # Use full ISO timestamp for enddate to capture all available data
    #params = {"startdate": py_dt_start.strftime('%Y-%m-%d'), "enddate": py_dt_end.strftime('%Y-%m-%d')}
    params = {"since": py_dt_start.strftime('%Y-%m-%dT00:00:00Z'), "_limit": 800}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching EA data: {e}")
        return {"items": []}


def get_shoothill_datatype(station_id: str) -> int:
    """Get the correct dataType for a Shoothill station from the API."""
    try:
        import config_keys
        SessionHeaderId = config_keys.SHOOTHILL_KEY
    except:
        print(f"Warning: Could not load SHOOTHILL_KEY from config_keys")
        return 3  # Default to 3 for backwards compatibility
    
    try:
        headers = {'content-type': 'application/json', 'SessionHeaderId': SessionHeaderId}
        url = f'http://riverlevelsapi.shoothill.com/TimeSeries/GetTimeSeriesStationById/?stationId={station_id}'
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        
        if 'gaugeList' in data and data['gaugeList']:
            gauge_list = data['gaugeList']
            if isinstance(gauge_list, str):
                gauge_list = json.loads(gauge_list)
            
            if isinstance(gauge_list, list) and len(gauge_list) > 0:
                dataTypeId = gauge_list[0].get('dataTypeId', 3)
                print(f"Station {station_id}: Using dataTypeId={dataTypeId}")
                return dataTypeId
    except Exception as e:
        print(f"Warning: Could not determine dataType for station {station_id}: {e}")
    
    return 3  # Default fallback


def fetch_shoothill_data(station_id: str, ndays: int = 1) -> dict:
    """Fetch readings from Shoothill API for the last `ndays` days."""
    if GAUGE is None:
        return None

    try:
        # Use ndays to compute date_start (inclusive) and date_end (now)
        date_end = np.datetime64('now')
        # subtract ndays days from date_end
        date_start = date_end - np.timedelta64(int(ndays), 'D')

        # Get the correct dataType for this station
        dataType = get_shoothill_datatype(station_id)

        gauge = GAUGE()
        dataset = gauge.read_shoothill_to_xarray(
            station_id=station_id,
            date_start=date_start,
            date_end=date_end,
            dataType=dataType
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


def get_station_data(station_key: str, ndays: int = 1) -> dict:
    """Fetch and format data for a specific station."""
    if station_key not in STATIONS:
        return {"error": "Station not found"}
    
    station = STATIONS[station_key]
    
    try:
        if station["source"] == "EA":
            data = fetch_station_data(station["id"], ndays=ndays)
            readings = [
                {
                    "dateTime": item["dateTime"],
                    "value": item["value"]
                }
                for item in data.get("items", [])
            ]
        elif station["source"] == "NRW":
            parameter_id = station.get("parameter_id", 41) # Default to 41 if not specified
            data = fetch_historical_data(station["id"], ndays=ndays, parameter=parameter_id)
            readings = [
                {
                    "dateTime": item["dateTime"],
                    "value": item["value"]
                }
                for item in data.get("items", [])
            ]
        #else:  # Shoothill
        #    dataset = fetch_shoothill_data(station["id"], ndays=ndays)
        #    readings = parse_shoothill_data(dataset)
        
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
    # allow client to request last N days via ?ndays=
    try:
        ndays = int(request.args.get('ndays', 1))
        if ndays < 1:
            ndays = 1
    except Exception:
        ndays = 1
    data = get_station_data(station_key, ndays=ndays)
    return jsonify(data)


@app.route("/api/all")
def api_all():
    """API endpoint for all stations data."""
    all_data = {}
    # allow a common ndays parameter for all stations
    try:
        ndays = int(request.args.get('ndays', 1))
        if ndays < 1:
            ndays = 1
    except Exception:
        ndays = 1
    for station_key in STATIONS.keys():
        all_data[station_key] = get_station_data(station_key, ndays=ndays)
    return jsonify(all_data)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
