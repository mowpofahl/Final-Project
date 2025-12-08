import json
from pathlib import Path

import requests

from db.db_operations import get_ingestion_index, store_air_quality_data, update_ingestion_index

API_KEY = "dbe0b483577a3246e7265d2b8270db24"
BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
COUNTIES_FILE = Path(__file__).resolve().parents[1] / "config" / "counties.json"
MAX_ROWS_PER_RUN = 25


def load_counties():
    """
    Load Colorado county metadata (lat/lon, FIPS) for API calls.
    """
    try:
        with open(COUNTIES_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if not isinstance(data, list):
                raise ValueError("counties.json must contain a list of county definitions.")
            return data
    except FileNotFoundError:
        raise FileNotFoundError("config/counties.json is missing; add Colorado counties to fetch data.")


def fetch_air_quality_data():
    """
    Fetch air quality data from OpenWeather API for a limited batch of counties.
    """
    counties = load_counties()
    if not counties:
        print("No counties configured; skipping air quality fetch.")
        return

    last_index = get_ingestion_index('air_quality')
    start_index = (last_index + 1) % len(counties)
    processed = 0
    idx = start_index
    visited = 0

    while processed < MAX_ROWS_PER_RUN and visited < len(counties):
        county = counties[idx]
        params = {'lat': county['lat'], 'lon': county['lon'], 'appid': API_KEY}
        response = requests.get(BASE_URL, params=params, timeout=30)
        if response.status_code == 200:
            payload = response.json()['list'][0]
            store_air_quality_data(
                county_name=county['county'],
                state_name=county['state'],
                fips=county['fips'],
                latitude=county['lat'],
                longitude=county['lon'],
                aqi=payload['main']['aqi'],
                pm25=payload['components']['pm2_5'],
                pm10=payload['components']['pm10'],
                co=payload['components']['co'],
                no2=payload['components']['no2'],
                so2=payload['components']['so2'],
                o3=payload['components']['o3'],
                timestamp=payload['dt'],
            )
            processed += 1
        else:
            print(f"Air quality request failed for {county['county']} County: {response.status_code} {response.text}")

        idx = (idx + 1) % len(counties)
        visited += 1

    final_index = (idx - 1) % len(counties)
    update_ingestion_index('air_quality', final_index)
