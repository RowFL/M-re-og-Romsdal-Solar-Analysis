import requests
import json
from datetime import datetime, timedelta
import time # Used for exponential backoff
import csv # New: Import the CSV module

# --- CONFIGURATION ---
# IMPORTANT: Client ID updated with the value you provided.
CLIENT_ID = "e4dd60bf-8c28-4ddf-8b37-baff9d45f05f"
BASE_URL = "https://frost.met.no"
API_VERSION = "v0" # Current stable version

# Define the 1-year period for extraction (Example: 2024-01-01 to 2025-01-01)
# Note: The Frost API uses an open-ended interval [start, end>
START_DATE = "2024-01-01"
END_DATE = "2025-01-01"
# The elements (weather parameters) you want to extract:
# Example: air_temperature (hourly), sum(precipitation_amount P1D) (daily sum)
ELEMENTS_TO_QUERY = 'air_temperature,sum(precipitation_amount P1D)'

# Max number of items the API returns per request
MAX_LIMIT = 10000 
# --- END CONFIGURATION ---


def authenticated_request(endpoint, params=None):
    """
    Handles authenticated GET requests to the Frost API with error checking and 
    implements exponential backoff for rate limiting (429 errors).
    """
    
    # Check for the required Client ID
    if CLIENT_ID == "YOUR_FROST_CLIENT_ID_HERE":
        print("!!! ERROR: Please replace 'YOUR_FROST_CLIENT_ID_HERE' with your actual Frost API Client ID.")
        return None
        
    url = f"{BASE_URL}/{endpoint}"
    # Basic authentication uses the Client ID as the username and an empty string as the password
    auth_tuple = (CLIENT_ID, '')

    max_retries = 5
    
    for attempt in range(max_retries):
        try:
            # We don't print the URL in full to keep the output clean, 
            # but indicate the purpose of the request.
            print(f"-> Requesting data from {endpoint.split('/')[0]}... (Attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, params=params, auth=auth_tuple, timeout=20)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            # Successfully retrieved and decoded JSON
            data = response.json()

            # The 'data' field is where observations and sources reside
            if 'data' not in data:
                print("Warning: 'data' field missing in response. Check API query.")
                return None
                
            return data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                print(f"Rate limit hit (429). Waiting for {wait_time} seconds before retrying...")
                if attempt + 1 == max_retries:
                    print("Max retries reached. Aborting request.")
                    return None
                time.sleep(wait_time)
            elif response.status_code in [401, 403]:
                print(f"Authentication Error ({response.status_code}): Check your Client ID.")
                return None
            else:
                print(f"HTTP Error: {e.response.status_code} - {e.response.text.splitlines()[0]}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the request: {e}")
            return None
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON response.")
            return None

    return None # Return None if all retries fail


def get_first_station_id():
    """Retrieves the ID of the first available weather station."""
    print("\n--- 1. FETCHING A SAMPLE STATION ID ---")
    
    endpoint = f"sources/{API_VERSION}.jsonld"
    params = {
        'types': 'SensorSystem', 
        'limit': 1 
    }
    
    result = authenticated_request(endpoint, params)
    
    if result and 'data' in result and result['data']:
        station_id = result['data'][0]['id']
        print(f"Successfully retrieved station ID: {station_id}")
        return station_id
    
    print("Error: Could not find any valid station ID.")
    return None


def get_observations_paginated(source_id, elements, start_date_str, end_date_str):
    """
    Extracts all observations for the given parameters within the time range,
    handling API pagination automatically.
    """
    print(f"\n--- 2. EXTRACTING OBSERVATIONS for {source_id} ({start_date_str} to {end_date_str}) ---")
    
    endpoint = f"observations/{API_VERSION}.jsonld"
    time_range = f"{start_date_str}/{end_date_str}"
    
    params = {
        'sources': source_id,
        'elements': elements,
        'referencetime': time_range,
        'timeoffsets': 'default',
        'levels': 'default',
        'limit': MAX_LIMIT,
        'offset': 0
    }

    all_observations = []
    page = 1
    
    while True:
        print(f"   -> Fetching page {page} (Offset: {params['offset']})...")
        result = authenticated_request(endpoint, params)

        if not result:
            break
            
        current_data = result.get('data', [])
        
        if not current_data:
            print("   -> No more data found. Pagination complete.")
            break
            
        all_observations.extend(current_data)
        
        # Check for more pages
        if len(current_data) < MAX_LIMIT:
            print(f"   -> Retrieved last page ({len(current_data)} items). Extraction finished.")
            break
        
        # Increment offset for the next page
        params['offset'] += MAX_LIMIT
        page += 1
        
    return all_observations


def process_observations(raw_data):
    """
    Flattens the nested observation data structure into a list of simpler dictionaries
    for easier use or conversion to CSV/DataFrame.
    """
    processed_list = []
    
    for item in raw_data:
        source_id = item.get('sourceId')
        reference_time = item.get('referenceTime')
        
        # Iterate over the nested 'observations' array within each referenceTime block
        for obs in item.get('observations', []):
            processed_list.append({
                'sourceId': source_id,
                'referenceTime': reference_time,
                'elementId': obs.get('elementId'),
                'value': obs.get('value'),
                'unit': obs.get('unit'),
                'timeOffset': obs.get('timeOffset'),
                'level': obs.get('level', {}).get('value'),
                'exposureCategory': obs.get('exposureCategory'),
                'qualityCode': obs.get('performanceCategory'), 
            })
            
    return processed_list


def save_to_csv(data, filename="frost_1year_data.csv"):
    """
    Saves the list of dictionaries to a CSV file.
    """
    if not data:
        print("Warning: No data to save.")
        return

    # Get the column names from the keys of the first dictionary
    fieldnames = list(data[0].keys())

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(data)
        
        print(f"\nSUCCESS: Data successfully saved to {filename}")

    except IOError:
        print(f"Error: Could not write to file {filename}")


if __name__ == '__main__':
    station_id = get_first_station_id()

    if station_id:
        raw_data = get_observations_paginated(
            source_id=station_id,
            elements=ELEMENTS_TO_QUERY,
            start_date_str=START_DATE,
            end_date_str=END_DATE
        )
        
        if raw_data:
            # Process the raw, nested JSON data into a clean, flat list
            flat_data = process_observations(raw_data)

            print("\n\n--- EXTRACTION COMPLETE ---")
            print(f"Total raw bundles retrieved: {len(raw_data)}")
            print(f"Total flattened observation records extracted: {len(flat_data)}")
            print(f"Data Source: {station_id}")
            print(f"Time Range: {START_DATE} to {END_DATE}")
            
            print("\n--- SAMPLE OF FLATTENED DATA (First 3 Records) ---")
            for record in flat_data[:3]:
                print(record)
                
            # --- NEW STEP: Save data to a CSV file ---
            save_to_csv(flat_data)
            # --- END NEW STEP ---