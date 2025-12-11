"""
FINAL WORKING SCRIPT: FROST (Solar/Temp) + OPEN-METEO (Hourly Snow)
FIXED: Adjusted date range to account for Open-Meteo Archive delay (5 days).
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

# Your Frost API Client ID (REQUIRED)
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f' 

# Date range: Last 1 year (shifted back 5 days to satisfy Archive API limits)
# The Archive API typically lags 5 days behind real-time.
END_DATE = datetime.now() - timedelta(days=5) 
START_DATE = END_DATE - timedelta(days=365*5)  # Last 5 years

# Stations & Coordinates
STATIONS = {
    'Tingvoll': {'id': 'SN64510', 'lat': 62.90, 'lon': 8.16},
    'Brusdalen': {'id': 'SN60875', 'lat': 62.46, 'lon': 6.88},
    'SURNADAL - SYLTE' : {'id': 'SN64760', 'lat': 63.07, 'lon': 8.93},
    'Linge': {'id': 'SN60650', 'lat': 62.47, 'lon': 7.23},
    'Vigra' : {'id': 'SN60990', 'lat': 62.56, 'lon': 6.10},
}

# Frost API Elements (Hourly)
FROST_HOURLY_ELEMENTS = [
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)',
    'air_temperature',
    'cloud_area_fraction',
]

# Open-Meteo API URL
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# ============================================================================
# 1. FROST API FUNCTION (Get Solar & Temp)
# ============================================================================

def get_frost_hourly_data(station_id, elements, start_date, end_date):
    """Downloads Hourly Temp and Radiation from Frost API."""
    url = 'https://frost.met.no/observations/v0.jsonld'
    
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    parameters = {
        'sources': station_id,
        'elements': ','.join(elements),
        'referencetime': f'{start_str}/{end_str}',
        'timeresolutions': 'PT1H',
        'levels': 'default'
    }
    
    print(f"  [Frost] Requesting Solar/Temp for {station_id}...")
    try:
        response = requests.get(url, parameters, auth=(CLIENT_ID, ''))
        if response.status_code == 200:
            data = response.json()
            if 'data' in data:
                records = []
                for obs in data['data']:
                    timestamp = obs['referenceTime']
                    record = {'timestamp': timestamp}
                    for observation in obs['observations']:
                        record[observation['elementId']] = observation['value']
                    records.append(record)
                
                df = pd.DataFrame(records)
                # Rename columns to friendly names
                df = df.rename(columns={
                    'mean(surface_downwelling_shortwave_flux_in_air PT1H)': 'global_radiation', 
                    'air_temperature': 'air_temperature_c',
                    'cloud_area_fraction': 'cloud_cover_percent'
                }, errors='ignore')
                
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                # Ensure UTC timezone awareness
                if df['timestamp'].dt.tz is None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
                else:
                    df['timestamp'] = df['timestamp'].dt.tz_convert('UTC')
                    
                return df
        print(f"  [Frost] Warning: No data or error {response.status_code}")
        return pd.DataFrame()
    except Exception as e:
        print(f"  [Frost] Error: {e}")
        return pd.DataFrame()

# ============================================================================
# 2. OPEN-METEO FUNCTION (Get Hourly Snow Depth)
# ============================================================================

def get_open_meteo_snow(lat, lon, start_date, end_date):
    """Downloads Hourly Snow Depth from Open-Meteo for any coordinate."""
    
    # Open-Meteo requires YYYY-MM-DD
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_str,
        "end_date": end_str,
        "hourly": "snow_depth", 
        "timezone": "UTC"
    }

    print(f"  [Open-Meteo] Requesting Hourly Snow Depth for ({lat}, {lon})...")
    
    try:
        response = requests.get(OPEN_METEO_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            
            # Extract hourly data
            hourly_data = data['hourly']
            df = pd.DataFrame({
                'timestamp': hourly_data['time'],
                'snow_depth_meters': hourly_data['snow_depth']
            })
            
            # Convert timestamp to datetime objects (UTC)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
            
            # Convert snow depth from meters to cm
            df['snow_depth_cm'] = df['snow_depth_meters'] * 100
            df = df.drop(columns=['snow_depth_meters'])
            
            return df
        else:
            print(f"  [Open-Meteo] Error {response.status_code}: {response.text}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"  [Open-Meteo] Connection Error: {e}")
        return pd.DataFrame()

# ============================================================================
# 3. MERGE AND RUN
# ============================================================================

def run_collection(stations, start_date, end_date, frost_elements):
    output_folder = 'final_weather_data'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created folder: {output_folder}")

    for name, info in stations.items():
        print(f"\nPROCESSING STATION: {name}")
        
        # 1. Get Frost Data (Temp/Solar)
        df_frost = get_frost_hourly_data(info['id'], frost_elements, start_date, end_date)
        
        # 2. Get Open-Meteo Data (Snow)
        df_snow = get_open_meteo_snow(info['lat'], info['lon'], start_date, end_date)
        
        if df_frost.empty:
            print(f"  Skipping {name}: Frost data failed.")
            continue
            
        if df_snow.empty:
            print(f"  Warning {name}: Snow data failed, filling with NaN.")
            df_final = df_frost.copy()
            df_final['snow_depth_cm'] = 0.0
        else:
            # 3. Merge on Timestamp
            df_final = pd.merge(df_frost, df_snow, on='timestamp', how='left')
            print(f"  Success: Merged {len(df_final)} records.")

        # Save to CSV
        filename = f"{output_folder}/{name.replace(' ', '_').lower()}_combined_data.csv"
        
        # Reorder columns nicely
        cols = ['timestamp', 'global_radiation', 'air_temperature_c', 'snow_depth_cm', 'cloud_cover_percent']
        available_cols = [c for c in cols if c in df_final.columns]
        df_final = df_final[available_cols]
        
        df_final.to_csv(filename, index=False)
        print(f"  SAVED: {filename}")
        
        time.sleep(1) # Be polite

if __name__ == "__main__":
    print("STARTING GUARANTEED DATA HARVEST...")
    run_collection(STATIONS, START_DATE, END_DATE, FROST_HOURLY_ELEMENTS)
    print("\nDONE.")