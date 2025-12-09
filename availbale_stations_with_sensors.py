import requests
import pandas as pd
import time

# Your Frost API credentials
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# Required elements
ELEMENTS = [
    'radiation_global',
    #'direct_radiation',
    #'diffuse_radiation',
    #'air_temperature',
    #'surface_snow_thickness',
    #'cloud_area_fraction'
]

# API endpoint for stations
stations_url = 'https://frost.met.no/sources/v0.jsonld'

parameters = {
    'county': 'Møre og Romsdal',
    'types': 'SensorSystem'
}

response = requests.get(stations_url, params=parameters, auth=(CLIENT_ID, ''))

if response.status_code == 200:
    stations = response.json()['data']
    print(f"Total stations found: {len(stations)}\n")

    valid_stations = []

    for station in stations:
        station_id = station['id']

        # Check available parameters for this station
        check_url = (
            f"https://frost.met.no/observations/availableTimeSeries/v0.jsonld"
            f"?sources={station_id}"
        )

        ts_response = requests.get(check_url, auth=(CLIENT_ID, ''))

        if ts_response.status_code != 200:
            continue

        ts_data = ts_response.json().get('data', [])

        available_elements = {item['elementId'] for item in ts_data}

        # ✅ Check if ALL required elements exist
        if all(elem in available_elements for elem in ELEMENTS):
            valid_stations.append({
                "Name": station['name'],
                "ID": station_id,
                "Location": station.get('geometry', {}).get('coordinates', 'N/A')
            })

        time.sleep(0.3)  # Prevent API rate limit

    # ✅ Print only matched stations
    if valid_stations:
        print(" Stations that support ALL required elements:\n")
        for s in valid_stations:
            print(f"Name: {s['Name']}")
            print(f"ID: {s['ID']}")
            print(f"Location: {s['Location']}")
            print("---")
    else:
        print(" No stations found with ALL required elements.")

else:
    print(f" Error fetching stations: {response.status_code}")
