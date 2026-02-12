########### Python 3.2 #############
import urllib.request, urllib.parse, json, os



try:
    NRW_KEY=os.environ["NRW_KEY"]
except KeyError:
    logging.info('Need a NRW API Key')


from datetime import datetime, timedelta

def fetch_historical_data(station_id, ndays=3, parameter=41):
    # Calculate date range
    to_date_dt = datetime.now()
    from_date_dt = to_date_dt - timedelta(days=ndays)
    
    # Format dates as strings (YYYY-MM-DD)
    to_date = to_date_dt.strftime('%Y-%m-%d')
    from_date = from_date_dt.strftime('%Y-%m-%d')

    base_url = "https://api.naturalresources.wales/rivers-and-seas/v1/api/StationData/historical"
    params = {
        "location": station_id,
        "parameter": parameter,
        "from": from_date,
        "to": to_date
    }
    query_string = urllib.parse.urlencode(params)
    url = f"{base_url}?{query_string}"
    
    hdr ={
    # Request headers
    'Cache-Control': 'no-cache',
    'Ocp-Apim-Subscription-Key': NRW_KEY,
    }

    req = urllib.request.Request(url, headers=hdr)
    req.get_method = lambda: 'GET'
    
    response = urllib.request.urlopen(req)
    # print(f"Status: {response.getcode()}")
    raw_data = json.loads(response.read())
    
    # Transform to match EA API structure (items list with dateTime and value)
    items = []
    if 'parameterReadings' in raw_data:
        for reading in raw_data['parameterReadings']:
            items.append({
                "dateTime": reading['time'],
                "value": reading['value']
            })
            
    return {"items": items}

try:
    # Example usage
    ndays = 3
    station_id = 4173 # NRW: Ironbridge
    
    data = fetch_historical_data(station_id, ndays, parameter=41)
    
    if 'items' in data and len(data['items']) > 0:
        readings = data['items']
        print(f"Count: {len(readings)}")
        print(f"First: {readings[0]}")
        print(f"Last:  {readings[-1]}")
    else:
        print("No readings found in items")

except Exception as e:
    print(e)
