import json
from pathlib import Path

import requests

from db.db_operations import store_air_quality_data

API_KEY = "dbe0b483577a3246e7265d2b8270db24"
BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
GEOCODE_URL = "https://api.openweathermap.org/geo/1.0/direct"
CITIES_FILE = Path(__file__).resolve().parents[1] / "config" / "cities.json"
DEFAULT_CITIES = ["Detroit,MI,USA", "Ann Arbor,MI,USA", "Chicago,IL,USA"]


def load_cities():
    """
    Load the list of cities from config/cities.json.
    Falls back to DEFAULT_CITIES if the file is missing or invalid.
    """
    try:
        with open(CITIES_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            if isinstance(data, list) and data:
                return data
            print("City config is empty; using default cities.")
    except FileNotFoundError:
        print("cities.json not found; using default cities.")
    except json.JSONDecodeError:
        print("cities.json is invalid; using default cities.")
    return DEFAULT_CITIES


def get_city_coordinates(city):
    """
    Use OpenWeather's Geocoding API to convert a city string into coordinates and metadata.
    Returns a dict containing latitude, longitude, canonical city name, state, and country.
    """
    params = {'q': city, 'limit': 1, 'appid': API_KEY}
    response = requests.get(GEOCODE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if data:
            location = data[0]
            return {
                'name': location.get('name', city),
                'state': location.get('state'),
                'country': location.get('country'),
                'lat': location.get('lat'),
                'lon': location.get('lon'),
            }
        print(f"No geocoding results for {city}.")
    else:
        print(f"Failed to geocode {city}: {response.status_code} {response.text}")
    return None


def fetch_air_quality_data():
    """
    Fetch air quality data from OpenWeather API.
    """
    cities = load_cities()
    for city in cities:
        location = get_city_coordinates(city)
        if not location:
            continue

        params = {'lat': location['lat'], 'lon': location['lon'], 'appid': API_KEY}
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            aqi = data['list'][0]['main']['aqi']
            pm25 = data['list'][0]['components']['pm2_5']
            pm10 = data['list'][0]['components']['pm10']
            co = data['list'][0]['components']['co']
            no2 = data['list'][0]['components']['no2']
            so2 = data['list'][0]['components']['so2']
            o3 = data['list'][0]['components']['o3']
            timestamp = data['list'][0]['dt']

            # Store data in the database
            store_air_quality_data(
                aqi,
                pm25,
                pm10,
                co,
                no2,
                so2,
                o3,
                location['lat'],
                location['lon'],
                location['name'],
                location.get('state'),
                location.get('country'),
                timestamp,
            )
        else:
            print(f"Failed to fetch data for {city}: {response.status_code} {response.text}")
