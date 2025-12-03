import requests

from api.fetch_air_quality import get_city_coordinates, load_cities
from db.db_operations import store_weather_data

API_KEY = "dbe0b483577a3246e7265d2b8270db24"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather_data():
    """
    Fetch weather data from OpenWeather API.
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
            temperature = data['main']['temp']
            humidity = data['main']['humidity']
            wind_speed = data['wind']['speed']
            pressure = data['main']['pressure']
            weather_description = data['weather'][0]['description']
            timestamp = data['dt']

            # Store data in the database
            store_weather_data(
                temperature,
                humidity,
                wind_speed,
                pressure,
                weather_description,
                location['lat'],
                location['lon'],
                location['name'],
                location.get('state'),
                location.get('country'),
                timestamp,
            )
        else:
            print(f"Failed to fetch weather data for {city}: {response.status_code} {response.text}")
