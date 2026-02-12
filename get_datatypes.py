"""
Debug script to get the correct dataType for queens_park
"""
import sys
import os
import json

try:
    import config_keys
    SessionHeaderId = config_keys.SHOOTHILL_KEY
except:
    print("ERROR: config_keys.py not found or SHOOTHILL_KEY not defined")
    sys.exit(1)

def get_station_dataType(station_id):
    """Get the correct dataType for a station from the Shoothill API"""
    import requests
    
    headers = {'content-type': 'application/json', 'SessionHeaderId': SessionHeaderId}
    
    # Get station info
    htmlcall_station_id = 'http://riverlevelsapi.shoothill.com/TimeSeries/GetTimeSeriesStationById/?stationId='
    url = htmlcall_station_id + str(station_id)
    
    request_raw = requests.get(url, headers=headers)
    header_dict = json.loads(request_raw.content)
    
    # Extract the dataTypeId from the gaugeList
    if 'gaugeList' in header_dict and header_dict['gaugeList']:
        # If gaugeList is a string, parse it
        gauge_list = header_dict['gaugeList']
        if isinstance(gauge_list, str):
            gauge_list = json.loads(gauge_list)
        
        if isinstance(gauge_list, list) and len(gauge_list) > 0:
            dataTypeId = gauge_list[0].get('dataTypeId')
            dataType_str = gauge_list[0].get('dataType')
            print(f"Station {station_id} ({header_dict['name']}):")
            print(f"  dataTypeId: {dataTypeId}")
            print(f"  dataType: {dataType_str}")
            return dataTypeId
    
    print(f"Could not find dataTypeId for station {station_id}")
    return None

if __name__ == "__main__":
    # Check the following stations
    stations = {
        "7708": "Liverpool (Gladstone Dock)",
        "7899": "Chester weir",
        "972": "Farndon",
        "968": "Ironbridge",
        "10831": "Queens Park"
    }
    
    print("Getting dataTypes for all stations:\n")
    for station_id, name in stations.items():
        try:
            dataType = get_station_dataType(station_id)
        except Exception as e:
            print(f"Error for station {station_id}: {e}")
