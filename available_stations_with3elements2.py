import requests
import pandas as pd
import time

# Frost API Client ID
CLIENT_ID = 'e4dd60bf-8c28-4ddf-8b37-baff9d45f05f'

# Full required elements
ELEMENTS = [
    'max(mean(surface_downwelling_shortwave_flux_in_air PT1M) PT1H)',
    'max(mean(surface_upwelling_longwave_flux_in_air PT1M) PT1H)',
    'mean(difference_between_downwelling_longwave_flux_and_instrument_longwave_radiation PT1M)',
    'mean(difference_between_downwelling_longwave_flux_and_instrument_longwave_radiation PT10M)',
    'mean(difference_between_upwelling_longwave_flux_and_instrument_longwave_radiation PT1M)',
    'mean(difference_between_upwelling_longwave_flux_and_instrument_longwave_radiation PT10M)',
    'mean(diffuse_downwelling_shortwave_flux_in_air P1D)',
    'mean(diffuse_downwelling_shortwave_flux_in_air PT1H)',
    'mean(diffuse_downwelling_shortwave_flux_in_air PT1M)',
    'mean(solar_irradiance PT1H)',
    'mean(solar_irradiance PT1M)',
    'mean(surface_albedo PT1H)',
    'mean(surface_downwelling_longwave_flux_in_air PT1H)',
    'mean(surface_downwelling_longwave_flux_in_air PT1M)',
    'mean(surface_downwelling_photosynthetic_radiative_flux_in_air PT1H)',
    'mean(surface_downwelling_shortwave_flux_in_air P1D)',
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)',
    'mean(surface_downwelling_shortwave_flux_in_air PT1M)',
    'mean(surface_downwelling_shortwave_flux_in_air PT10M)',
    'mean(surface_net_downward_radiative_flux PT1H)',
    'mean(surface_net_downward_radiative_flux PT1M)',
    'mean(surface_upwelling_longwave_flux_in_air PT1H)',
    'mean(surface_upwelling_longwave_flux_in_air PT1M)',
    'mean(surface_upwelling_shortwave_flux_in_air P1D)',
    'mean(surface_upwelling_shortwave_flux_in_air PT1H)',
    'mean(surface_upwelling_shortwave_flux_in_air PT1M)',
    'integral_of_direct_normal_irradiance(PT1H)',
    'integral_of_diffuse_horizontal_irradiance(PT1H)',
    'air_temperature',
    'surface_snow_thickness',
    'cloud_area_fraction'
]

# Radiation-only elements
RADIATION_ELEMENTS = [
    'max(mean(surface_downwelling_shortwave_flux_in_air PT1M) PT1H)',
    'max(mean(surface_upwelling_longwave_flux_in_air PT1M) PT1H)',
    'mean(difference_between_downwelling_longwave_flux_and_instrument_longwave_radiation PT1M)',
    'mean(difference_between_downwelling_longwave_flux_and_instrument_longwave_radiation PT10M)',
    'mean(difference_between_upwelling_longwave_flux_and_instrument_longwave_radiation PT1M)',
    'mean(difference_between_upwelling_longwave_flux_and_instrument_longwave_radiation PT10M)',
    'mean(diffuse_downwelling_shortwave_flux_in_air P1D)',
    'mean(diffuse_downwelling_shortwave_flux_in_air PT1H)',
    'mean(diffuse_downwelling_shortwave_flux_in_air PT1M)',
    'mean(solar_irradiance PT1H)',
    'mean(solar_irradiance PT1M)',
    'mean(surface_albedo PT1H)',
    'mean(surface_downwelling_longwave_flux_in_air PT1H)',
    'mean(surface_downwelling_longwave_flux_in_air PT1M)',
    'mean(surface_downwelling_photosynthetic_radiative_flux_in_air PT1H)',
    'mean(surface_downwelling_shortwave_flux_in_air P1D)',
    'mean(surface_downwelling_shortwave_flux_in_air PT1H)',
    'mean(surface_downwelling_shortwave_flux_in_air PT1M)',
    'mean(surface_downwelling_shortwave_flux_in_air PT10M)',
    'mean(surface_net_downward_radiative_flux PT1H)',
    'mean(surface_net_downward_radiative_flux PT1M)',
    'mean(surface_upwelling_longwave_flux_in_air PT1H)',
    'mean(surface_upwelling_longwave_flux_in_air PT1M)',
    'mean(surface_upwelling_shortwave_flux_in_air P1D)',
    'mean(surface_upwelling_shortwave_flux_in_air PT1H)',
    'mean(surface_upwelling_shortwave_flux_in_air PT1M)'
]

# Stations API
stations_url = 'https://frost.met.no/sources/v0.jsonld'

params = {
    'county': 'Møre og Romsdal',
    'types': 'SensorSystem'
}

response = requests.get(stations_url, params=params, auth=(CLIENT_ID, ''))

if response.status_code != 200:
    print("ERROR: Failed to fetch stations:", response.status_code)
    exit()

stations = response.json()['data']
print("Total stations found:", len(stations))

valid_stations = []

# API-safe delay (4 requests per second)
DELAY = 0.3

for idx, station in enumerate(stations, start=1):
    station_id = station['id']

    ts_url = "https://frost.met.no/observations/availableTimeSeries/v0.jsonld"
    ts_params = {"sources": station_id}

    ts_response = requests.get(ts_url, params=ts_params, auth=(CLIENT_ID, ''))

    if ts_response.status_code != 200:
        time.sleep(DELAY)
        continue

    series = ts_response.json().get('data', [])
    available_elements = {s['elementId'] for s in series}

    matched_all = set(ELEMENTS).intersection(available_elements)
    matched_radiation = set(RADIATION_ELEMENTS).intersection(available_elements)

    rule_1_pass = len(matched_all) >= 3
    rule_2_pass = len(matched_radiation) >= 1

    if rule_1_pass or rule_2_pass:
        selection_reason = []
        if rule_1_pass:
            selection_reason.append("At least 3 core elements")
        if rule_2_pass:
            selection_reason.append("Has radiation data")

        valid_stations.append({
            "Name": station['name'],
            "ID": station_id,
            "Longitude": station.get('geometry', {}).get('coordinates', ['N/A', 'N/A'])[0],
            "Latitude": station.get('geometry', {}).get('coordinates', ['N/A', 'N/A'])[1],
            "Matched_All_Elements": ", ".join(sorted(matched_all)),
            "Matched_Radiation_Elements": ", ".join(sorted(matched_radiation)),
            "Match_Count": len(matched_all),
            "Selection_Reason": " + ".join(selection_reason)
        })

    print("Checked", idx, "of", len(stations), ":", station_id)
    time.sleep(DELAY)

# Export to CSV
df = pd.DataFrame(valid_stations)
csv_filename = "valid_frost_stations_smart_filter_new.csv"
df.to_csv(csv_filename, index=False)

print("\nProcessing complete")
print("Valid stations found:", len(valid_stations))
print("CSV exported as:", csv_filename)
