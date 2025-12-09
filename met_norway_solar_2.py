"""
MET Norway Frost API - Simple Data Downloader
Downloads weather data and saves to CSV files
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Your Frost API Client ID
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# Weather stations in Møre og Romsdal
STATIONS = {
    'Tingvoll': 'SN64510',
    'Brusdalen': 'SN60875',
    'Surnadal-Sylte': 'SN64760',
    'Linge': 'SN60650',
    'Vigra': 'SN60990',
}

# Date range
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=365*10)  # Last 10 years

# Weather parameters
ELEMENTS = [
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)',  # Solar radiation
    'air_temperature',                                        # Temperature
    'over_time(thickness_of_snowfall_amount P1D)',                     # Snow depth
    'cloud_area_fraction'                                    # Cloud cover
]

# Settings
BATCH_SIZE_DAYS = 365  # Download in 1-year chunks
REQUEST_DELAY = 2      # Seconds between requests
MAX_RETRIES = 3        # Retry attempts on failure
OUTPUT_FOLDER = 'met_norway_data'
OUTPUT_FORMAT = 'csv'  # Options: 'csv' or 'excel'

# Column name mapping for cleaner output
COLUMN_MAPPING = {
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)': 'solar_radiation_w_m2',
    'air_temperature': 'air_temperature_c',
    'mean(surface_snow_thickness PT1H)': 'snow_depth_cm',
    'cloud_area_fraction': 'cloud_cover_percent'
}

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_weather_data(station_id, start_date, end_date, elements):
    """Download weather data from Frost API."""
    
    url = 'https://frost.met.no/observations/v0.jsonld'
    
    # Format dates for API
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # API parameters
    parameters = {
        'sources': station_id,
        'elements': ','.join(elements),
        'referencetime': f'{start_str}/{end_str}',
        'timeresolutions': 'PT1H',
    }
    
    # Make request
    response = requests.get(url, parameters, auth=(CLIENT_ID, ''), timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        if 'data' in data and len(data['data']) > 0:
            return parse_frost_data(data['data'])
    
    return pd.DataFrame()


def parse_frost_data(observations):
    """Convert API JSON to DataFrame."""
    
    records = []
    
    for obs in observations:
        timestamp = obs['referenceTime']
        record = {'timestamp': timestamp}
        
        if 'observations' in obs:
            for observation in obs['observations']:
                element = observation['elementId']
                value = observation.get('value', None)
                record[element] = value
        
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    return df


def download_with_retry(station_id, start_date, end_date, elements, max_retries):
    """Download with automatic retry."""
    
    for attempt in range(max_retries):
        try:
            df = get_weather_data(station_id, start_date, end_date, elements)
            if not df.empty:
                return df
        except Exception as e:
            print(f"  Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(5)
    
    return pd.DataFrame()


def download_station(station_id, station_name, start_date, end_date, elements, batch_size_days):
    """Download data for one station in batches."""
    
    print(f"\n{'='*60}")
    print(f"Station: {station_name} ({station_id})")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"{'='*60}")
    
    all_batches = []
    current_start = start_date
    batch_num = 1
    
    # Calculate total batches
    total_days = (end_date - start_date).days
    total_batches = (total_days // batch_size_days) + 1
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=batch_size_days), end_date)
        
        print(f"Batch {batch_num}/{total_batches}: {current_start.date()} to {current_end.date()}")
        
        df_batch = download_with_retry(station_id, current_start, current_end, elements, MAX_RETRIES)
        
        if not df_batch.empty:
            print(f"  [OK] Downloaded {len(df_batch)} records")
            all_batches.append(df_batch)
        else:
            print(f"  [WARNING] No data")
        
        current_start = current_end
        batch_num += 1
        
        if current_start < end_date:
            time.sleep(REQUEST_DELAY)
    
    # Combine batches
    if all_batches:
        df_combined = pd.concat(all_batches, ignore_index=True)
        
        # Clean column names
        df_combined = df_combined.rename(columns=COLUMN_MAPPING)
        
        # Add station info
        df_combined['station_name'] = station_name
        df_combined['station_id'] = station_id
        
        # Remove duplicates
        df_combined = df_combined.drop_duplicates(subset=['timestamp'], keep='first')
        
        print(f"[OK] Total: {len(df_combined)} records")
        return df_combined
    
    print(f"[ERROR] No data downloaded")
    return pd.DataFrame()


def save_data(all_data, output_folder, output_format):
    """Save data to files."""
    
    # Create folder
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    print(f"\n{'='*60}")
    print("SAVING FILES")
    print(f"{'='*60}")
    
    saved_files = []
    
    for station_name, df in all_data.items():
        if df.empty:
            continue
        
        # Create filename
        safe_name = station_name.replace(' ', '_').replace('-', '_').lower()
        
        if output_format == 'excel':
            filename = f"{safe_name}_hourly_data.xlsx"
            filepath = os.path.join(output_folder, filename)
            df.to_excel(filepath, index=False, engine='openpyxl')
        else:  # csv
            filename = f"{safe_name}_hourly_data.csv"
            filepath = os.path.join(output_folder, filename)
            df.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
        
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"[OK] {filename} - {len(df):,} rows - {file_size:.2f} MB")
        saved_files.append(filepath)
    
    return saved_files


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    
    print("="*70)
    print("MET NORWAY DATA DOWNLOADER")
    print("="*70)
    
    print(f"\nConfiguration:")
    print(f"  Stations: {len(STATIONS)}")
    print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"  Duration: {(END_DATE - START_DATE).days / 365.25:.1f} years")
    print(f"  Output format: {OUTPUT_FORMAT.upper()}")
    print(f"  Output folder: {OUTPUT_FOLDER}")
    
    # Start download
    print(f"\n{'='*70}")
    print("STARTING DOWNLOAD")
    print(f"{'='*70}")
    
    start_time = time.time()
    all_data = {}
    
    for idx, (station_name, station_id) in enumerate(STATIONS.items(), 1):
        print(f"\n[{idx}/{len(STATIONS)}]")
        
        df = download_station(
            station_id, 
            station_name, 
            START_DATE, 
            END_DATE, 
            ELEMENTS, 
            BATCH_SIZE_DAYS
        )
        
        if not df.empty:
            all_data[station_name] = df
        
        if idx < len(STATIONS):
            time.sleep(REQUEST_DELAY)
    
    # Save files
    if all_data:
        saved_files = save_data(all_data, OUTPUT_FOLDER, OUTPUT_FORMAT)
        
        # Summary
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print(f"\n{'='*70}")
        print("COMPLETE")
        print(f"{'='*70}")
        print(f"Stations: {len(all_data)}/{len(STATIONS)}")
        print(f"Total records: {sum(len(df) for df in all_data.values()):,}")
        print(f"Files: {len(saved_files)}")
        print(f"Time: {minutes}m {seconds}s")
        print(f"Location: {os.path.abspath(OUTPUT_FOLDER)}")
    else:
        print("\n[ERROR] No data downloaded!")