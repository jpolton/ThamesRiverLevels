
import sys
import os
import json

# Add current directory to path so we can import nrw_api
sys.path.append(os.getcwd())

from nrw_api.nrw_api import fetch_historical_data

def test_stations():
    farndon_id = "4170"
    ironbridge_id = "4173"
    
    print(f"Fetching data for Farndon (ID: {farndon_id})...")
    farndon_data = fetch_historical_data(farndon_id, ndays=1, parameter=40)
    
    print(f"Fetching data for Ironbridge (ID: {ironbridge_id})...")
    ironbridge_data = fetch_historical_data(ironbridge_id, ndays=1, parameter=41)
    
    print("\n--- Comparison ---")
    
    farndon_items = farndon_data.get('items', [])
    ironbridge_items = ironbridge_data.get('items', [])
    
    print(f"Farndon items count: {len(farndon_items)}")
    print(f"Ironbridge items count: {len(ironbridge_items)}")
    
    if len(farndon_items) > 0:
        print(f"Farndon first item: {farndon_items[0]}")
    if len(ironbridge_items) > 0:
        print(f"Ironbridge first item: {ironbridge_items[0]}")
        
    if farndon_items == ironbridge_items:
        print("\nDATA IS IDENTICAL!")
    else:
        print("\nData is different.")

if __name__ == "__main__":
    test_stations()
