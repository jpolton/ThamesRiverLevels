"""
Debug script to print available data for queens_park station
"""
import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime

# Add shoothill_api to path
sys.path.append(os.path.dirname(os.path.abspath("shoothill_api/shoothill_api.py")))

try:
    from shoothill_api import GAUGE
except ImportError:
    from shoothill_api.shoothill_api import GAUGE

def debug_queens_park():
    """Fetch and print all available data for queens_park"""
    station_id = "10831"
    
    print(f"\n{'='*60}")
    print(f"Fetching data for queens_park (ID: {station_id})")
    print(f"{'='*60}\n")
    
    try:
        # Fetch raw API response to inspect structure
        import requests
        import json
        
        try:
            import config_keys
            SessionHeaderId = config_keys.SHOOTHILL_KEY
        except:
            print("ERROR: config_keys.py not found or SHOOTHILL_KEY not defined")
            return
        
        headers = {'content-type': 'application/json', 'SessionHeaderId': SessionHeaderId}
        
        # Get station info
        htmlcall_station_id = 'http://riverlevelsapi.shoothill.com/TimeSeries/GetTimeSeriesStationById/?stationId='
        url = htmlcall_station_id + str(station_id)
        print(f"\nFetching station info from: {url}")
        request_raw = requests.get(url, headers=headers)
        header_dict = json.loads(request_raw.content)
        print(f"\nStation info response:")
        print(json.dumps(header_dict, indent=2))
        
        # Get data
        ndays = 1
        date_end = np.datetime64('now')
        date_start = np.datetime64(datetime.now().date().isoformat())
        
        startTime = date_start.item().strftime('%Y-%m-%dT%H:%M:%SZ')
        endTime = date_end.item().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        htmlcall_station_id = 'http://riverlevelsapi.shoothill.com/TimeSeries/GetTimeSeriesDatapointsDateTime/?stationId='
        url = htmlcall_station_id + str(station_id) + '&dataType=3&endTime=' + endTime + '&startTime=' + startTime
        
        print(f"\n\nFetching data from: {url}")
        request_raw = requests.get(url, headers=headers)
        request = json.loads(request_raw.content)
        print(f"\nData response:")
        print(json.dumps(request, indent=2))
        
        print(f"\n\nAttempting to parse with GAUGE class...")
        
        gauge = GAUGE()
        dataset = gauge.read_shoothill_to_xarray(
            station_id=station_id,
            date_start=date_start,
            date_end=date_end
        )
        
        if dataset is None:
            print("ERROR: Failed to retrieve dataset")
            return
        
        print(f"Dataset type: {type(dataset)}")
        print(f"\nDataset dimensions:\n{dataset.dims}")
        print(f"\nDataset variables:\n{dataset.data_vars}")
        print(f"\nDataset coordinates:\n{dataset.coords}")
        print(f"\nDataset attributes:\n{dataset.attrs}")
        
        # Convert to dataframe for easier inspection
        df = dataset.to_dataframe().reset_index()
        print(f"\n\nDataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nFirst few rows:\n{df.head(10)}")
        print(f"\nData types:\n{df.dtypes}")
        print(f"\nMissing values:\n{df.isnull().sum()}")
        print(f"\nDataFrame info:")
        print(df.info())
        print(f"\nFull DataFrame:\n{df.to_string()}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_queens_park()
