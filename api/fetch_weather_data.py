import requests

from api.fetch_air_quality import load_counties
from db.db_operations import get_ingestion_index, store_weather_data, update_ingestion_index

API_KEY = "dbe0b483577a3246e7265d2b8270db24"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
MAX_ROWS_PER_RUN = 25


def fetch_weather_data():
    """
    Fetch weather data from OpenWeather API for a limited batch of Colorado counties.
    """
    counties = load_counties()
    if not counties:
        print("No counties configured; skipping weather fetch.")
        return

    last_index = get_ingestion_index('weather')
    start_index = (last_index + 1) % len(counties)
    processed = 0
    idx = start_index
    visited = 0

    while processed < MAX_ROWS_PER_RUN and visited < len(counties):
        county = counties[idx]
        params = {'lat': county['lat'], 'lon': county['lon'], 'appid': API_KEY, 'units': 'imperial'}
        response = requests.get(BASE_URL, params=params, timeout=30)
        if response.status_code == 200:
            payload = response.json()
            store_weather_data(
                county_name=county['county'],
                state_name=county['state'],
                fips=county['fips'],
                latitude=county['lat'],
                longitude=county['lon'],
                temperature=payload['main']['temp'],
                humidity=payload['main']['humidity'],
                wind_speed=payload['wind']['speed'],
                pressure=payload['main']['pressure'],
                description=payload['weather'][0]['description'],
                timestamp=payload['dt'],
            )
            processed += 1
        else:
            print(f"Weather request failed for {county['county']} County: {response.status_code} {response.text}")

        idx = (idx + 1) % len(counties)
        visited += 1

    final_index = (idx - 1) % len(counties)
    update_ingestion_index('weather', final_index)
