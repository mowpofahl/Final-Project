import sqlite3
import time
from pathlib import Path

from db.db_setup import create_db

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def _get_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_or_create_county(cursor, county_name, state_name, fips=None, latitude=None, longitude=None):
    cursor.execute(
        '''
        SELECT id FROM counties
        WHERE name = ? AND state = ?
    ''',
        (county_name, state_name),
    )
    row = cursor.fetchone()
    if row:
        county_id = row[0]
        cursor.execute(
            '''
            UPDATE counties
            SET fips = COALESCE(?, fips), latitude = COALESCE(?, latitude), longitude = COALESCE(?, longitude)
            WHERE id = ?
        ''',
            (fips, latitude, longitude, county_id),
        )
        return county_id

    cursor.execute(
        '''
        INSERT INTO counties (name, state, fips, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
    ''',
        (county_name, state_name, fips, latitude, longitude),
    )
    return cursor.lastrowid


def store_air_quality_data(
    county_name,
    state_name,
    fips,
    latitude,
    longitude,
    aqi,
    pm25,
    pm10,
    co,
    no2,
    so2,
    o3,
    observed_at,
):
    conn = _get_connection()
    cursor = conn.cursor()
    county_id = _get_or_create_county(cursor, county_name, state_name, fips, latitude, longitude)
    ingested_at = int(time.time())
    cursor.execute(
        '''
        INSERT OR IGNORE INTO air_quality (
            county_id,
            aqi,
            pm25,
            pm10,
            co,
            no2,
            so2,
            o3,
            timestamp,
            observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
        (county_id, aqi, pm25, pm10, co, no2, so2, o3, ingested_at, observed_at),
    )
    conn.commit()
    conn.close()


def store_health_data(
    county_name,
    state_name,
    fips,
    asthma_rate,
    lower_ci,
    upper_ci,
    visits,
    gender,
    year,
):
    conn = _get_connection()
    cursor = conn.cursor()
    county_id = _get_or_create_county(cursor, county_name, state_name, fips)
    cursor.execute(
        '''
        INSERT OR IGNORE INTO health_data (
            county_id,
            asthma_rate,
            lower_ci,
            upper_ci,
            visits,
            gender,
            year
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
        (county_id, asthma_rate, lower_ci, upper_ci, visits, gender, year),
    )
    inserted = cursor.rowcount
    conn.commit()
    conn.close()
    return inserted > 0


def store_weather_data(
    county_name,
    state_name,
    fips,
    latitude,
    longitude,
    temperature,
    humidity,
    wind_speed,
    pressure,
    description,
    observed_at,
):
    conn = _get_connection()
    cursor = conn.cursor()
    county_id = _get_or_create_county(cursor, county_name, state_name, fips, latitude, longitude)
    ingested_at = int(time.time())
    cursor.execute(
        '''
        INSERT OR IGNORE INTO weather_data (
            county_id,
            temperature,
            humidity,
            wind_speed,
            pressure,
            description,
            timestamp,
            observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''',
        (county_id, temperature, humidity, wind_speed, pressure, description, ingested_at, observed_at),
    )
    conn.commit()
    conn.close()


def store_data_in_db():
    """
    Ensure the database and tables exist before storing.
    """
    create_db()
