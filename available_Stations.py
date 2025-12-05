import requests
import pandas as pd

# Your Frost API credentials
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'  # Replace with your actual ID

# API endpoint for finding stations
url = 'https://frost.met.no/sources/v0.jsonld'

# Parameters: we want stations in Møre og Romsdal county
parameters = {
    'county': 'Møre og Romsdal',
    'types': 'SensorSystem'  # Weather stations
}

# Make the request
response = requests.get(url, parameters, auth=(CLIENT_ID, ''))

# Convert to readable format
if response.status_code == 200:
    data = response.json()
    stations = data['data']
    
    # Show station information
    for station in stations:
        print(f"Name: {station['name']}")
        print(f"ID: {station['id']}")
        print(f"Location: {station.get('geometry', {}).get('coordinates', 'N/A')}")
        print("---")
else:
    print(f"Error: {response.status_code}")