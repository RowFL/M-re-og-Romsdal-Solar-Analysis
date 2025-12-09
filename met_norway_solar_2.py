"""
MET Norway Frost API - Solar Radiation Data Collection (OPTIMIZED)
For solar potential analysis in Møre og Romsdal

Optimizations:
- Batch processing for large date ranges
- Better error handling and retry logic
- Data validation and quality checks
- Automatic column name standardization
- Progress tracking
- Memory-efficient processing
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Your Frost API Client ID (get this from frost.met.no)
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# Weather stations in Møre og Romsdal
STATIONS = {
    'Tingvoll': 'SN64510',
    'Brusdalen': 'SN60875',
    'Surnadal-Sylte': 'SN64760',
    'Linge': 'SN60650',
    'Vigra': 'SN60990',
}

# Date range: Last 5 years (can adjust)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=365*10)

# Weather parameters with correct element names
ELEMENTS = [
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)',  # Global solar radiation
    'air_temperature',                                        # Temperature (°C)
    'mean(surface_snow_thickness PT1H)',                     # Snow depth (cm)
    'cloud_area_fraction'                                    # Cloud cover (%)
]

# Column name mapping for cleaner output
COLUMN_MAPPING = {
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)': 'solar_radiation_w_m2',
    'air_temperature': 'air_temperature_c',
    'mean(surface_snow_thickness PT1H)': 'snow_depth_cm',
    'cloud_area_fraction': 'cloud_cover_percent'
}

# API Configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
REQUEST_DELAY = 2  # seconds between requests
BATCH_SIZE_DAYS = 365  # Download in 1-year chunks

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_output_folder(folder_name: str = 'met_norway_data') -> str:
    """Create output folder if it doesn't exist."""
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f" Created folder: {folder_name}")
    return folder_name


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to more readable names."""
    df = df.rename(columns=COLUMN_MAPPING)
    return df


def validate_data(df: pd.DataFrame, station_name: str) -> Tuple[pd.DataFrame, Dict]:
    """
    Validate and clean the downloaded data.
    
    Returns:
    - Cleaned DataFrame
    - Dictionary with validation statistics
    """
    if df.empty:
        return df, {'status': 'empty'}
    
    initial_count = len(df)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['timestamp'], keep='first')
    duplicates_removed = initial_count - len(df)
    
    # Sort by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Check for gaps in time series
    time_diff = df['timestamp'].diff()
    expected_interval = pd.Timedelta(hours=1)
    gaps = (time_diff > expected_interval * 2).sum()
    
    # Data quality statistics
    stats = {
        'status': 'valid',
        'total_records': len(df),
        'duplicates_removed': duplicates_removed,
        'time_gaps': gaps,
        'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
    }
    
    # Check each parameter
    for col in df.columns:
        if col not in ['timestamp', 'station_name', 'station_id']:
            missing = df[col].isna().sum()
            stats[f'{col}_missing'] = missing
            stats[f'{col}_missing_pct'] = (missing / len(df)) * 100
    
    return df, stats


# ============================================================================
# DATA DOWNLOAD FUNCTIONS
# ============================================================================

def get_weather_data_with_retry(
    station_id: str, 
    start_date: datetime, 
    end_date: datetime, 
    elements: List[str],
    max_retries: int = MAX_RETRIES
) -> pd.DataFrame:
    """
    Download weather data with automatic retry on failure.
    
    Parameters:
    - station_id: MET Norway station ID
    - start_date: Start date
    - end_date: End date
    - elements: List of weather parameters
    - max_retries: Maximum number of retry attempts
    
    Returns:
    - pandas DataFrame with hourly data
    """
    
    for attempt in range(max_retries):
        try:
            df = get_weather_data(station_id, start_date, end_date, elements)
            if not df.empty:
                return df
            else:
                print(f"  Attempt {attempt + 1}/{max_retries}: No data returned")
        except Exception as e:
            print(f"  Attempt {attempt + 1}/{max_retries}: Error - {str(e)}")
        
        if attempt < max_retries - 1:
            print(f"  Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    print(f"  Failed after {max_retries} attempts")
    return pd.DataFrame()


def get_weather_data(
    station_id: str, 
    start_date: datetime, 
    end_date: datetime, 
    elements: List[str]
) -> pd.DataFrame:
    """
    Download weather data from Frost API for one station and time period.
    """
    
    url = 'https://frost.met.no/observations/v0.jsonld'
    
    # Format dates for API (ISO 8601 format)
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    # API parameters
    parameters = {
        'sources': station_id,
        'elements': ','.join(elements),
        'referencetime': f'{start_str}/{end_str}',
        'timeresolutions': 'PT1H',
    }
    
    # Make the API request
    response = requests.get(url, parameters, auth=(CLIENT_ID, ''), timeout=30)
    
    # Check if request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Check if we got any data
        if 'data' in data and len(data['data']) > 0:
            df = parse_frost_data(data['data'])
            return df
        else:
            return pd.DataFrame()
    elif response.status_code == 404:
        print(f" No data available (404)")
        return pd.DataFrame()
    elif response.status_code == 412:
        print(f"Request too large (412) - try smaller date range")
        return pd.DataFrame()
    else:
        raise Exception(f"API Error {response.status_code}: {response.text[:200]}")


def parse_frost_data(observations: List) -> pd.DataFrame:
    """
    Convert Frost API JSON response to a clean pandas DataFrame.
    Handles missing data and multiple observations per timestamp.
    """
    
    records = []
    
    for obs in observations:
        timestamp = obs['referenceTime']
        
        # Extract all measurements from this observation
        record = {'timestamp': timestamp}
        
        if 'observations' in obs:
            for observation in obs['observations']:
                element = observation['elementId']
                value = observation.get('value', None)
                record[element] = value
        
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)  # Remove timezone
    
    # Sort by time
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    return df


def download_station_in_batches(
    station_id: str,
    station_name: str,
    start_date: datetime,
    end_date: datetime,
    elements: List[str],
    batch_size_days: int = BATCH_SIZE_DAYS
) -> pd.DataFrame:
    """
    Download data in batches to handle large date ranges.
    This prevents API timeouts and memory issues.
    """
    
    print(f"\n{'='*60}")
    print(f"Processing: {station_name} ({station_id})")
    print(f"{'='*60}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    all_batches = []
    current_start = start_date
    batch_num = 1
    
    # Calculate total batches
    total_days = (end_date - start_date).days
    total_batches = (total_days // batch_size_days) + 1
    
    while current_start < end_date:
        # Calculate batch end date
        current_end = min(current_start + timedelta(days=batch_size_days), end_date)
        
        print(f"\nBatch {batch_num}/{total_batches}: {current_start.date()} to {current_end.date()}")
        
        # Download this batch
        df_batch = get_weather_data_with_retry(
            station_id, current_start, current_end, elements
        )
        
        if not df_batch.empty:
            print(f"  Downloaded {len(df_batch)} records")
            all_batches.append(df_batch)
        else:
            print(f"  No data for this period")
        
        # Move to next batch
        current_start = current_end
        batch_num += 1
        
        # Be polite to the API
        if current_start < end_date:
            time.sleep(REQUEST_DELAY)
    
    # Combine all batches
    if all_batches:
        df_combined = pd.concat(all_batches, ignore_index=True)
        
        # Standardize column names
        df_combined = standardize_column_names(df_combined)
        
        # Add station information
        df_combined['station_name'] = station_name
        df_combined['station_id'] = station_id
        
        print(f"\nTotal records downloaded: {len(df_combined)}")
        return df_combined
    else:
        print(f"\n No data downloaded for {station_name}")
        return pd.DataFrame()


def download_all_stations(
    stations: Dict[str, str],
    start_date: datetime,
    end_date: datetime,
    elements: List[str]
) -> Dict[str, pd.DataFrame]:
    """
    Download data from multiple weather stations with progress tracking.
    """
    
    all_data = {}
    successful = 0
    failed = 0
    
    for idx, (station_name, station_id) in enumerate(stations.items(), 1):
        print(f"\n{'#'*70}")
        print(f"STATION {idx}/{len(stations)}")
        print(f"{'#'*70}")
        
        try:
            df = download_station_in_batches(
                station_id, station_name, start_date, end_date, elements
            )
            
            if not df.empty:
                # Validate data
                df_clean, validation_stats = validate_data(df, station_name)
                
                print(f"\n--- Data Quality Report ---")
                print(f"  Records after cleaning: {validation_stats['total_records']}")
                print(f"  Duplicates removed: {validation_stats['duplicates_removed']}")
                print(f"  Time gaps detected: {validation_stats['time_gaps']}")
                
                all_data[station_name] = df_clean
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            print(f"\n CRITICAL ERROR processing {station_name}: {str(e)}")
            failed += 1
        
        # Pause between stations
        if idx < len(stations):
            time.sleep(REQUEST_DELAY)
    
    print(f"\n{'='*70}")
    print(f"DOWNLOAD COMPLETE: {successful} successful, {failed} failed")
    print(f"{'='*70}")
    
    return all_data


# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def calculate_solar_statistics(df: pd.DataFrame, station_name: str) -> Optional[Dict]:
    """
    Calculate comprehensive statistics from solar radiation data.
    """
    
    solar_col = 'solar_radiation_w_m2'
    
    if solar_col not in df.columns or df[solar_col].isna().all():
        print(f"  No solar radiation data available for {station_name}")
        return None
    
    # Filter out missing values
    df_solar = df[df[solar_col].notna()].copy()
    
    if len(df_solar) == 0:
        return None
    
    # Convert W/m² to kWh/m² (hourly values)
    df_solar['solar_radiation_kwh'] = df_solar[solar_col] / 1000
    
    # Extract date components
    df_solar['year'] = df_solar['timestamp'].dt.year
    df_solar['month'] = df_solar['timestamp'].dt.month
    df_solar['hour'] = df_solar['timestamp'].dt.hour
    
    # Calculate statistics
    stats = {
        'station': station_name,
        'total_records': len(df_solar),
        'date_range': f"{df_solar['timestamp'].min().date()} to {df_solar['timestamp'].max().date()}",
        'years_covered': (df_solar['timestamp'].max() - df_solar['timestamp'].min()).days / 365.25,
        
        # Hourly statistics
        'avg_hourly_radiation_w': df_solar[solar_col].mean(),
        'max_hourly_radiation_w': df_solar[solar_col].max(),
        'median_hourly_radiation_w': df_solar[solar_col].median(),
        
        # Annual statistics
        'annual_avg_kwh_m2': df_solar.groupby('year')['solar_radiation_kwh'].sum().mean(),
        'annual_min_kwh_m2': df_solar.groupby('year')['solar_radiation_kwh'].sum().min(),
        'annual_max_kwh_m2': df_solar.groupby('year')['solar_radiation_kwh'].sum().max(),
        
        # Monthly statistics
        'monthly_avg_kwh_m2': df_solar.groupby('month')['solar_radiation_kwh'].sum().mean(),
        
        # Seasonal statistics (Northern Hemisphere)
        'winter_avg_kwh_m2': df_solar[df_solar['month'].isin([12,1,2])]['solar_radiation_kwh'].sum() / (len(df_solar['year'].unique()) * 3),
        'spring_avg_kwh_m2': df_solar[df_solar['month'].isin([3,4,5])]['solar_radiation_kwh'].sum() / (len(df_solar['year'].unique()) * 3),
        'summer_avg_kwh_m2': df_solar[df_solar['month'].isin([6,7,8])]['solar_radiation_kwh'].sum() / (len(df_solar['year'].unique()) * 3),
        'autumn_avg_kwh_m2': df_solar[df_solar['month'].isin([9,10,11])]['solar_radiation_kwh'].sum() / (len(df_solar['year'].unique()) * 3),
        
        # Peak sun hours (radiation > 200 W/m²)
        'avg_peak_sun_hours_per_day': (df_solar[solar_col] > 200).sum() / len(df_solar['timestamp'].dt.date.unique()),
    }
    
    return stats


def save_data(all_data: Dict[str, pd.DataFrame], output_folder: str = 'met_norway_data'):
    """
    Save downloaded data to CSV files with metadata.
    """
    
    folder = create_output_folder(output_folder)
    saved_files = []
    
    print(f"\n{'='*70}")
    print("SAVING DATA FILES")
    print(f"{'='*70}")
    
    for station_name, df in all_data.items():
        if df.empty:
            continue
        
        # Create safe filename
        safe_name = station_name.replace(' ', '_').replace('-', '_').lower()
        filename = f"{safe_name}_hourly_data.csv"
        filepath = os.path.join(folder, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False, date_format='%Y-%m-%d %H:%M:%S')
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        
        print(f" {filename}")
        print(f"    Records: {len(df):,} | Size: {file_size:.2f} MB")
        
        saved_files.append(filepath)
    
    # Create summary file
    summary_path = os.path.join(folder, '_download_summary.txt')
    with open(summary_path, 'w') as f:
        f.write("MET NORWAY DATA DOWNLOAD SUMMARY\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Download Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Date Range: {START_DATE.date()} to {END_DATE.date()}\n")
        f.write(f"Stations: {len(all_data)}\n")
        f.write(f"Parameters: {', '.join([COLUMN_MAPPING.get(e, e) for e in ELEMENTS])}\n\n")
        f.write("Files Created:\n")
        for fp in saved_files:
            f.write(f"  - {os.path.basename(fp)}\n")
    
    print(f"\nSummary saved: {summary_path}")
    return saved_files


def generate_statistics_report(all_data: Dict[str, pd.DataFrame], output_folder: str = 'met_norway_data'):
    """
    Generate a comprehensive statistics report for all stations.
    """
    
    print(f"\n{'='*70}")
    print("GENERATING STATISTICS REPORT")
    print(f"{'='*70}")
    
    all_stats = []
    
    for station_name, df in all_data.items():
        stats = calculate_solar_statistics(df, station_name)
        if stats:
            all_stats.append(stats)
            
            print(f"\n{station_name}:")
            print(f"  Annual average: {stats['annual_avg_kwh_m2']:.1f} kWh/m²/year")
            print(f"  Summer monthly: {stats['summer_avg_kwh_m2']:.1f} kWh/m²/month")
            print(f"  Winter monthly: {stats['winter_avg_kwh_m2']:.1f} kWh/m²/month")
            print(f"  Peak sun hours: {stats['avg_peak_sun_hours_per_day']:.1f} hours/day")
    
    # Save statistics to CSV
    if all_stats:
        stats_df = pd.DataFrame(all_stats)
        stats_path = os.path.join(output_folder, 'solar_statistics_summary.csv')
        stats_df.to_csv(stats_path, index=False)
        print(f"\n Statistics saved: {stats_path}")
        
        return stats_df
    
    return None


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("MET NORWAY FROST API - SOLAR RADIATION DATA COLLECTION")
    print("OPTIMIZED VERSION WITH BATCH PROCESSING & VALIDATION")
    print("="*70)
    
    print(f"\n Configuration:")
    print(f"  Stations: {len(STATIONS)} - {', '.join(STATIONS.keys())}")
    print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"  Duration: {(END_DATE - START_DATE).days / 365.25:.1f} years")
    print(f"  Parameters: {len(ELEMENTS)}")
    for elem in ELEMENTS:
        clean_name = COLUMN_MAPPING.get(elem, elem)
        print(f"    - {clean_name}")
    
    print(f"\n  Processing settings:")
    print(f"  Batch size: {BATCH_SIZE_DAYS} days")
    print(f"  Request delay: {REQUEST_DELAY} seconds")
    print(f"  Max retries: {MAX_RETRIES}")
    
    # Confirm before starting
    #input("\n  Press ENTER to start download, or Ctrl+C to cancel...")
    
    # Start timer
    start_time = time.time()
    
    # Download data from all stations
    print("\n" + "="*70)
    print("STARTING DATA DOWNLOAD")
    print("="*70)
    
    all_data = download_all_stations(STATIONS, START_DATE, END_DATE, ELEMENTS)
    
    if not all_data:
        print("\n ERROR: No data downloaded from any station!")
        print("  Check your station IDs and date range.")
        exit(1)
    
    # Save all data
    saved_files = save_data(all_data)
    
    # Generate statistics report
    stats_df = generate_statistics_report(all_data)
    
    # Calculate execution time
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    # Final summary
    print("\n" + "="*70)
    print("✓ALL OPERATIONS COMPLETE")
    print("="*70)
    print(f"\n Summary:")
    print(f"  Stations processed: {len(all_data)}/{len(STATIONS)}")
    print(f"  Total records: {sum(len(df) for df in all_data.values()):,}")
    print(f"  Files created: {len(saved_files)}")
    print(f"  Execution time: {minutes}m {seconds}s")
    
    print(f"\n Output location: {os.path.abspath('met_norway_data')}")
    
    print("\n Next steps:")
    print("  1. Review the CSV files in the 'met_norway_data' folder")
    print("  2. Check 'solar_statistics_summary.csv' for station comparison")
    print("  3. Examine '_download_summary.txt' for download details")
    print("  4. Use this data to calibrate your GIS solar radiation model")
    
    print("\n" + "="*70)