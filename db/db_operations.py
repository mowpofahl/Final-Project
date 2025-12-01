import sqlite3

from db.db_setup import create_db

DB_FILE = "project.db"


def store_air_quality_data(aqi, pm25, pm10, co, no2, so2, o3, lat, lon, city, timestamp):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO air_quality (aqi, pm25, pm10, co, no2, so2, o3, lat, lon, city, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
        (aqi, pm25, pm10, co, no2, so2, o3, lat, lon, city, timestamp),
    )
    conn.commit()
    conn.close()


def store_health_data(state, asthma_rate, copd_rate, year):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO health_data (state, asthma_rate, copd_rate, year)
        VALUES (?, ?, ?, ?)
    ''',
        (state, asthma_rate, copd_rate, year),
    )
    conn.commit()
    conn.close()


def store_weather_data(
    temperature, humidity, wind_speed, pressure, weather_description, lat, lon, city, timestamp
):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO weather_data (temperature, humidity, wind_speed, pressure, weather_description, lat, lon, city, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
        (
            temperature,
            humidity,
            wind_speed,
            pressure,
            weather_description,
            lat,
            lon,
            city,
            timestamp,
        ),
    )
    conn.commit()
    conn.close()


def store_data_in_db():
    """
    Placeholder to ensure the database and tables exist before storing.
    """
    create_db()
