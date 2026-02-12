"""
Test script to verify all stations' data can be fetched
"""
import sys
import os
sys.path.insert(0, '/Users/jelt/GitHub/DeeRiverLevels')

from flask_app import get_station_data, STATIONS

print("\n" + "="*70)
print("Testing data fetch for ALL stations")
print("="*70 + "\n")

for station_key, station_info in STATIONS.items():
    print(f"\n{station_key.upper()}")
    print("-" * 50)
    
    result = get_station_data(station_key,ndays=7)
    
    print(f"Name: {result['station']['name']}")
    print(f"Source: {result['station']['source']}")
    
    if "error" in result:
        print(f"ERROR: {result['error']}")
    else:
        stats = result.get('stats', {})
        readings = result.get('readings', [])
        print(f"Number of readings: {len(readings)}")
        if len(readings) > 0:
            print(f"Min value: {stats.get('min', 'N/A')}")
            print(f"Max value: {stats.get('max', 'N/A')}")
            print(f"Mean value: {stats.get('mean', 'N/A'):.4f}")
            print(f"Latest: {readings[-1]['dateTime']} = {readings[-1]['value']}")
