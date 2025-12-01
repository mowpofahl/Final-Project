import requests

from db.db_operations import store_weather_data

API_KEY = "your_api_key_here"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"


def fetch_weather_data():
    """
    Fetch weather data from OpenWeather API.
    """
    cities = ["Detroit", "Ann Arbor", "Chicago"]  # Example cities
    for city in cities:
        params = {'q': city, 'appid': API_KEY}
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            temperature = data['main']['temp']
            humidity = data['main']['humidity']
            wind_speed = data['wind']['speed']
            pressure = data['main']['pressure']
            weather_description = data['weather'][0]['description']
            lat = data['coord']['lat']
            lon = data['coord']['lon']
            timestamp = data['dt']

            # Store data in the database
            store_weather_data(
                temperature,
                humidity,
                wind_speed,
                pressure,
                weather_description,
                lat,
                lon,
                city,
                timestamp,
            )
        else:
            print(f"Failed to fetch weather data for {city}")
