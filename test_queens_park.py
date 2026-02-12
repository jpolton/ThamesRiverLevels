"""
Test script to verify queens_park data can now be fetched
"""
import sys
import os
sys.path.insert(0, '/Users/jelt/GitHub/DeeRiverLevels')

from flask_app import get_station_data

print("\n" + "="*70)
print("Testing data fetch for queens_park")
print("="*70 + "\n")

result = get_station_data("queens_park")

print(f"\nStation: {result['station']['name']}")
print(f"Source: {result['station']['source']}")

if "error" in result:
    print(f"ERROR: {result['error']}")
else:
    stats = result.get('stats', {})
    readings = result.get('readings', [])
    print(f"\nNumber of readings: {len(readings)}")
    print(f"Min value: {stats.get('min', 'N/A')}")
    print(f"Max value: {stats.get('max', 'N/A')}")
    print(f"Mean value: {stats.get('mean', 'N/A')}")
    
    if readings:
        print(f"\nLatest readings:")
        for reading in readings[-5:]:
            print(f"  {reading['dateTime']}: {reading['value']}")
