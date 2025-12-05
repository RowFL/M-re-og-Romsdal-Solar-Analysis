import requests
import pandas as pd
import time

# ✅ Frost API Client ID
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# ✅ Required elements
ELEMENTS = [
    'global_radiation',
    'direct_radiation',
    'diffuse_radiation',
    'air_temperature',
    'surface_snow_thickness',
    'cloud_area_fraction'
]

# ✅ Stations API
stations_url = 'https://frost.met.no/sources/v0.jsonld'

params = {
    'county': 'Møre og Romsdal',
    'types': 'SensorSystem'
}

response = requests.get(stations_url, params=params, auth=(CLIENT_ID, ''))

if response.status_code != 200:
    print(f" Failed to fetch stations: {response.status_code}")
    exit()

stations = response.json()['data']
print(f" Total stations found: {len(stations)}")

valid_stations = []

# ✅ API-safe delay (4 requests per second)
DELAY = 0.3

for idx, station in enumerate(stations, start=1):
    station_id = station['id']

    ts_url = "https://frost.met.no/observations/availableTimeSeries/v0.jsonld"
    ts_params = {"sources": station_id}

    ts_response = requests.get(ts_url, params=ts_params, auth=(CLIENT_ID, ''))

    if ts_response.status_code != 200:
        print(f"⚠ Skipping {station_id} (status {ts_response.status_code})")
        time.sleep(DELAY)
        continue

    series = ts_response.json().get('data', [])
    available_elements = {s['elementId'] for s in series}

    # ✅ Count matching elements
    matched_elements = set(ELEMENTS).intersection(available_elements)

    if len(matched_elements) >= 3:
        valid_stations.append({
            "Name": station['name'],
            "ID": station_id,
            "Longitude": station.get('geometry', {}).get('coordinates', ['N/A', 'N/A'])[0],
            "Latitude": station.get('geometry', {}).get('coordinates', ['N/A', 'N/A'])[1],
            "Matched_Elements": ", ".join(sorted(matched_elements)),
            "Match_Count": len(matched_elements)
        })

    print(f"Checked {idx}/{len(stations)} -> {station_id}")
    time.sleep(DELAY)

# ✅ Export to CSV
df = pd.DataFrame(valid_stations)
csv_filename = "valid_frost_stations_min_3_elements.csv"
df.to_csv(csv_filename, index=False)

print("\nProcessing complete")
print(f" Valid stations found (min 3 elements): {len(valid_stations)}")
print(f" CSV exported as: {csv_filename}")
