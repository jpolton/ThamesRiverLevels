
import requests
import json
import sys
import os

# Try to get API key from config_keys
try:
    sys.path.append(os.getcwd())
    import config_keys
    API_KEY = config_keys.NRW_KEY
except ImportError:
    print("Error: Could not import config_keys.NRW_KEY")
    sys.exit(1)

def check_endpoint(url):
    print(f"Checking URL: {url}")
    headers = {
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': API_KEY,
    }
    try:
        response = requests.get(url, headers=headers)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            try:
                data = response.json()
                print("Response starts with:")
                print(str(data)[:500])
                return data
            except:
                print("Response is not JSON")
                print(response.text[:500])
        else:
            print(f"Error: {response.text[:200]}")
    except Exception as e:
        print(f"Exception: {e}")

def main():
    base_url = "https://api.naturalresources.wales/rivers-and-seas/v1/api/StationData"
    
    # Try to get list of stations
    stations = check_endpoint(base_url)
    
    if stations and isinstance(stations, list):
        target_ids = [4170, 4173]
        for s in stations:
            # Check various fields for the ID (it might be an integer or string)
            s_id = s.get('stationId')
            s_loc = s.get('location')
            
            if s_id in target_ids or s_loc in target_ids:
                print(f"\nFOUND STATION {s_id} / {s_loc}:")
                print(json.dumps(s, indent=2))


if __name__ == "__main__":
    main()
