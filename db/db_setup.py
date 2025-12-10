import sqlite3

DB_FILE = "project.db"


def create_db():
    """
    Keep the database schema ready for storing API data.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    # ingestion_state is no longer needed now that inserts rely on UNIQUE constraints.
    cursor.execute('DROP TABLE IF EXISTS ingestion_state')

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
            UNIQUE(county_id, timestamp),
            FOREIGN KEY(county_id) REFERENCES counties(id)
        )
    '''
    )

    conn.commit()
    conn.close()
