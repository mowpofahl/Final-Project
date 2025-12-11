import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def _ensure_column(cursor, table, column_name, definition):
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name in columns:
        return False
    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
    return True


def create_db():
    """
    Keep the database schema ready for storing API data.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS counties (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            fips TEXT UNIQUE,
            latitude REAL,
            longitude REAL,
            UNIQUE(name, state)
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY,
            county_id INTEGER NOT NULL,
            aqi INTEGER,
            pm25 REAL,
            pm10 REAL,
            co REAL,
            no2 REAL,
            so2 REAL,
            o3 REAL,
            timestamp INTEGER,
            observed_at INTEGER,
            UNIQUE(county_id, timestamp),
            FOREIGN KEY(county_id) REFERENCES counties(id)
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            county_id INTEGER NOT NULL,
            asthma_rate REAL,
            lower_ci REAL,
            upper_ci REAL,
            visits INTEGER,
            gender TEXT,
            year INTEGER,
            UNIQUE(county_id, gender, year),
            FOREIGN KEY(county_id) REFERENCES counties(id)
        )
    '''
    )

    air_added = _ensure_column(cursor, 'air_quality', 'observed_at', 'INTEGER')
    if air_added:
        cursor.execute('UPDATE air_quality SET observed_at = timestamp WHERE observed_at IS NULL')

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY,
            county_id INTEGER NOT NULL,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            pressure REAL,
            timestamp INTEGER,
            observed_at INTEGER,
            description TEXT,
            UNIQUE(county_id, timestamp),
            FOREIGN KEY(county_id) REFERENCES counties(id)
        )
    '''
    )

    weather_added = _ensure_column(cursor, 'weather_data', 'observed_at', 'INTEGER')
    if weather_added:
        cursor.execute('UPDATE weather_data SET observed_at = timestamp WHERE observed_at IS NULL')

    conn.commit()
    conn.close()
