import sqlite3

from db.db_setup import create_db

DB_FILE = "project.db"


def _get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_or_create_state(cursor, state_name):
    if not state_name:
        return None

    cursor.execute('SELECT id FROM states WHERE name = ?', (state_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute('INSERT INTO states (name) VALUES (?)', (state_name,))
    return cursor.lastrowid


def _get_or_create_city(cursor, city_name, state_name, country, latitude, longitude):
    state_id = _get_or_create_state(cursor, state_name)
    if state_id is None:
        if country is None:
            cursor.execute(
                '''
                SELECT id FROM cities
                WHERE name = ? AND state_id IS NULL AND country IS NULL
            ''',
                (city_name,),
            )
        else:
            cursor.execute(
                '''
                SELECT id FROM cities
                WHERE name = ? AND state_id IS NULL AND country = ?
            ''',
                (city_name, country),
            )
    else:
        if country is None:
            cursor.execute(
                '''
                SELECT id FROM cities
                WHERE name = ? AND state_id = ? AND country IS NULL
            ''',
                (city_name, state_id),
            )
        else:
            cursor.execute(
                '''
                SELECT id FROM cities
                WHERE name = ? AND state_id = ? AND country = ?
            ''',
                (city_name, state_id, country),
            )
    row = cursor.fetchone()
    if row:
        city_id = row[0]
        cursor.execute(
            '''
            UPDATE cities
            SET latitude = COALESCE(?, latitude), longitude = COALESCE(?, longitude)
            WHERE id = ?
        ''',
            (latitude, longitude, city_id),
        )
        return city_id

    cursor.execute(
        '''
        INSERT INTO cities (name, state_id, country, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
    ''',
        (city_name, state_id, country, latitude, longitude),
    )
    return cursor.lastrowid


def _get_or_create_weather_condition(cursor, description):
    if not description:
        return None

    cursor.execute('SELECT id FROM weather_conditions WHERE description = ?', (description,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute('INSERT INTO weather_conditions (description) VALUES (?)', (description,))
    return cursor.lastrowid


def store_air_quality_data(
    aqi, pm25, pm10, co, no2, so2, o3, lat, lon, city_name, state_name, country, timestamp
):
    conn = _get_connection()
    cursor = conn.cursor()
    city_id = _get_or_create_city(cursor, city_name, state_name, country, lat, lon)
    cursor.execute(
        '''
        INSERT INTO air_quality (city_id, aqi, pm25, pm10, co, no2, so2, o3, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
        (city_id, aqi, pm25, pm10, co, no2, so2, o3, timestamp),
    )
    conn.commit()
    conn.close()


def store_health_data(state_name, asthma_rate, copd_rate, year):
    conn = _get_connection()
    cursor = conn.cursor()
    state_id = _get_or_create_state(cursor, state_name)
    cursor.execute(
        '''
        INSERT INTO health_data (state_id, asthma_rate, copd_rate, year)
        VALUES (?, ?, ?, ?)
    ''',
        (state_id, asthma_rate, copd_rate, year),
    )
    conn.commit()
    conn.close()


def store_weather_data(
    temperature,
    humidity,
    wind_speed,
    pressure,
    weather_description,
    lat,
    lon,
    city_name,
    state_name,
    country,
    timestamp,
):
    conn = _get_connection()
    cursor = conn.cursor()
    city_id = _get_or_create_city(cursor, city_name, state_name, country, lat, lon)
    condition_id = _get_or_create_weather_condition(cursor, weather_description)
    cursor.execute(
        '''
        INSERT INTO weather_data (
            city_id,
            temperature,
            humidity,
            wind_speed,
            pressure,
            weather_condition_id,
            timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
        (city_id, temperature, humidity, wind_speed, pressure, condition_id, timestamp),
    )
    conn.commit()
    conn.close()


def store_data_in_db():
    """
    Ensure the database and tables exist before storing.
    """
    create_db()
