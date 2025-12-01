import sqlite3

DB_FILE = "project.db"


def create_db():
    """
    This function creates the SQLite database and the necessary tables for storing data.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create tables for air quality, health data, and weather data
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY,
            aqi INTEGER,
            pm25 REAL,
            pm10 REAL,
            co REAL,
            no2 REAL,
            so2 REAL,
            o3 REAL,
            lat REAL,
            lon REAL,
            city TEXT,
            timestamp INTEGER
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            state TEXT,
            asthma_rate REAL,
            copd_rate REAL,
            year INTEGER
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            pressure REAL,
            weather_description TEXT,
            lat REAL,
            lon REAL,
            city TEXT,
            timestamp INTEGER
        )
    '''
    )

    conn.commit()
    conn.close()
