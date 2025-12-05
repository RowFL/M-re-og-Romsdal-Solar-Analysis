"""
MET Norway Frost API - Solar Radiation Data Collection
For solar potential analysis in Møre og Romsdal

This script downloads hourly solar radiation, temperature, and snow data
from MET Norway weather stations for the past 10 years.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Your Frost API Client ID (get this from frost.met.no)
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# Weather stations in Møre og Romsdal
STATIONS = {
    'Molde': 'SN62270',      # MOLDE LUFTHAVN
    #'Oslo': 'SN90450',      # Oslo Blindern
    #'Ålesund': 'SN60947',    # Ålesund lufthavn
    'Vigra' : 'SN60990',    # Ålesund Vigra
    'Kristiansund': 'SN64330' # Kristiansund lufthavn
}

# Date range: Last 10 years
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=365*1)

# Weather parameters we need for solar analysis
ELEMENTS = [
    'global_radiation',           # Total solar radiation (W/m²)
    'direct_radiation',           # Direct beam radiation
    'diffuse_radiation',          # Scattered/diffuse radiation
    'air_temperature',            # Temperature (°C)
    'surface_snow_thickness',     # Snow depth (cm)
    'cloud_area_fraction'         # Cloud cover (%)
]

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_weather_data(station_id, start_date, end_date, elements):
    """
    Download weather data from Frost API for one station
    
    Parameters:
    - station_id: MET Norway station ID (e.g., 'SN62910')
    - start_date: Start date (datetime object)
    - end_date: End date (datetime object)
    - elements: List of weather parameters to download
    
    Returns:
    - pandas DataFrame with hourly data
    """
    
    # Frost API endpoint for observations
    url = 'https://frost.met.no/observations/v0.jsonld'
    
    # Format dates for API (ISO 8601 format)
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    # API parameters
    parameters = {
        'sources': station_id,
        'elements': ','.join(elements),
        'referencetime': f'{start_str}/{end_str}',
        'timeresolutions': 'PT1H',  # PT1H = hourly data
        'levels': 'default'
    }
    
    print(f"Requesting data from {station_id}...")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Make the API request
    response = requests.get(url, parameters, auth=(CLIENT_ID, ''))
    
    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Check if we got any data
        if 'data' in data and len(data['data']) > 0:
            print(f"SUCCESS: Downloaded {len(data['data'])} observations")
            
            # Convert to pandas DataFrame
            df = parse_frost_data(data['data'])
            return df
        else:
            print(f"WARNING: No data available for this station/period")
            return pd.DataFrame()
    else:
        print(f"ERROR {response.status_code}: {response.text}")
        return pd.DataFrame()


def parse_frost_data(observations):
    """
    Convert Frost API JSON response to a clean pandas DataFrame
    
    Parameters:
    - observations: List of observations from Frost API
    
    Returns:
    - pandas DataFrame with columns for each weather element
    """
    
    records = []
    
    for obs in observations:
        timestamp = obs['referenceTime']
        
        # Extract all measurements from this observation
        record = {'timestamp': timestamp}
        
        for observation in obs['observations']:
            element = observation['elementId']
            value = observation['value']
            record[element] = value
        
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Sort by time
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    return df


def download_all_stations(stations, start_date, end_date, elements):
    """
    Download data from multiple weather stations
    
    Parameters:
    - stations: Dictionary of {station_name: station_id}
    - start_date, end_date: Date range
    - elements: Weather parameters to download
    
    Returns:
    - Dictionary of {station_name: DataFrame}
    """
    
    all_data = {}
    
    for station_name, station_id in stations.items():
        print(f"\n{'='*60}")
        print(f"Processing: {station_name}")
        print(f"{'='*60}")
        
        # Download data
        df = get_weather_data(station_id, start_date, end_date, elements)
        
        if not df.empty:
            # Add station identifier
            df['station_name'] = station_name
            df['station_id'] = station_id
            
            all_data[station_name] = df
        
        # Be polite to the API - wait 2 seconds between requests
        time.sleep(2)
    
    return all_data


def calculate_solar_statistics(df):
    """
    Calculate useful statistics from solar radiation data
    
    Parameters:
    - df: DataFrame with solar radiation data
    
    Returns:
    - Dictionary with summary statistics
    """
    
    if 'global_radiation' not in df.columns:
        return None
    
    # Convert W/m² to kWh/m² (divide by 1000, multiply by hours)
    df['global_radiation_kwh'] = df['global_radiation'] / 1000
    
    stats = {
        'total_years': (df['timestamp'].max() - df['timestamp'].min()).days / 365,
        'average_hourly_radiation_w': df['global_radiation'].mean(),
        'max_radiation_w': df['global_radiation'].max(),
        'annual_total_kwh_m2': df.groupby(df['timestamp'].dt.year)['global_radiation_kwh'].sum().mean(),
        'monthly_average_kwh_m2': df.groupby(df['timestamp'].dt.month)['global_radiation_kwh'].sum().mean(),
        'winter_average_kwh_m2': df[df['timestamp'].dt.month.isin([12,1,2])].groupby(df['timestamp'].dt.month)['global_radiation_kwh'].sum().mean(),
        'summer_average_kwh_m2': df[df['timestamp'].dt.month.isin([6,7,8])].groupby(df['timestamp'].dt.month)['global_radiation_kwh'].sum().mean()
    }
    
    return stats


def save_data(all_data, output_folder='met_norway_data'):
    """
    Save downloaded data to CSV files
    
    Parameters:
    - all_data: Dictionary of {station_name: DataFrame}
    - output_folder: Folder name to save files
    """
    
    import os
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"\nCreated folder: {output_folder}")
    
    for station_name, df in all_data.items():
        # Create safe filename
        filename = f"{station_name.replace(' ', '_').lower()}_hourly_data.csv"
        filepath = os.path.join(output_folder, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        print(f"SAVED: {filepath} ({len(df)} rows)")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    
    print("="*70)
    print("MET NORWAY FROST API - SOLAR RADIATION DATA COLLECTION")
    print("="*70)
    
    # Check if Client ID is set
    """ if CLIENT_ID == 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f':
        print("\nWARNING: Please set your CLIENT_ID in the configuration section!")
        print("Get your Client ID from: https://frost.met.no")
        exit()
     """
    print(f"\nConfiguration:")
    print(f"  Stations: {list(STATIONS.keys())}")
    print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"  Parameters: {', '.join(ELEMENTS)}")
    
    # Download data from all stations
    print("\n" + "="*70)
    print("STARTING DATA DOWNLOAD")
    print("="*70)
    
    all_data = download_all_stations(STATIONS, START_DATE, END_DATE, ELEMENTS)
    
    # Display results and statistics
    print("\n" + "="*70)
    print("DOWNLOAD SUMMARY")
    print("="*70)
    
    for station_name, df in all_data.items():
        print(f"\n{station_name}:")
        print(f"  Records: {len(df)}")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"  Available parameters: {[col for col in df.columns if col not in ['timestamp', 'station_name', 'station_id']]}")
        
        # Calculate solar statistics if available
        stats = calculate_solar_statistics(df)
        if stats:
            print(f"\n  Solar Radiation Statistics:")
            print(f"    Annual average: {stats['annual_total_kwh_m2']:.1f} kWh/m²/year")
            print(f"    Summer average: {stats['summer_average_kwh_m2']:.1f} kWh/m²/month")
            print(f"    Winter average: {stats['winter_average_kwh_m2']:.1f} kWh/m²/month")
    
    # Save all data
    print("\n" + "="*70)
    print("SAVING DATA")
    print("="*70)
    
    save_data(all_data)
    
    print("\nDONE: All done! Your data is ready for analysis.")
    print("\nNext steps:")
    print("  1. Open the CSV files in Excel or pandas")
    print("  2. Check data quality and completeness")
    print("  3. Use this data to calibrate your solar radiation model")
