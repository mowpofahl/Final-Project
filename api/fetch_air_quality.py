import requests

from db.db_operations import store_air_quality_data

API_KEY = "your_api_key_here"
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"


def fetch_air_quality_data():
    """
    Fetch air quality data from OpenWeather API.
    """
    cities = ["Detroit", "Ann Arbor", "Chicago"]  # Example cities
    for city in cities:
        params = {'q': city, 'appid': API_KEY}
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
            lat = data['city']['coord']['lat']
            lon = data['city']['coord']['lon']
            timestamp = data['list'][0]['dt']

            # Store data in the database
            store_air_quality_data(aqi, pm25, pm10, co, no2, so2, o3, lat, lon, city, timestamp)
        else:
            print(f"Failed to fetch data for {city}")
