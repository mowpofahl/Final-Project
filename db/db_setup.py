import sqlite3

DB_FILE = "project.db"


def create_db():
    """
    Create or migrate the SQLite database for the Colorado county-centric project.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='counties'"
    )
    schema_ready = cursor.fetchone() is not None

    if not schema_ready:
        # Legacy tables are dropped so we can rebuild a normalized county-centric schema.
        cursor.execute('DROP TABLE IF EXISTS air_quality')
        cursor.execute('DROP TABLE IF EXISTS weather_data')
        cursor.execute('DROP TABLE IF EXISTS health_data')
        cursor.execute('DROP TABLE IF EXISTS cities')
        cursor.execute('DROP TABLE IF EXISTS states')
        cursor.execute('DROP TABLE IF EXISTS weather_conditions')

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
        CREATE TABLE IF NOT EXISTS ingestion_state (
            source TEXT PRIMARY KEY,
            last_index INTEGER NOT NULL DEFAULT -1
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
            description TEXT,
            FOREIGN KEY(county_id) REFERENCES counties(id)
        )
    '''
    )

    conn.commit()
    conn.close()
