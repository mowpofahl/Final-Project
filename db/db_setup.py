import sqlite3

DB_FILE = "project.db"


def _table_has_column(cursor, table_name, column_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return any(row[1] == column_name for row in cursor.fetchall())


def create_db():
    """
    Create or migrate the SQLite database so string fields are normalized into ID tables.
    Existing tables that are missing the new normalized columns are dropped and recreated.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    if not _table_has_column(cursor, 'air_quality', 'city_id'):
        cursor.execute('DROP TABLE IF EXISTS air_quality')
    if not _table_has_column(cursor, 'weather_data', 'city_id'):
        cursor.execute('DROP TABLE IF EXISTS weather_data')
    if not _table_has_column(cursor, 'weather_data', 'weather_condition_id'):
        cursor.execute('DROP TABLE IF EXISTS weather_data')
    if not _table_has_column(cursor, 'health_data', 'state_id'):
        cursor.execute('DROP TABLE IF EXISTS health_data')

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS states (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            state_id INTEGER,
            country TEXT,
            latitude REAL,
            longitude REAL,
            UNIQUE(name, state_id, country),
            FOREIGN KEY(state_id) REFERENCES states(id)
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS weather_conditions (
            id INTEGER PRIMARY KEY,
            description TEXT UNIQUE
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS air_quality (
            id INTEGER PRIMARY KEY,
            city_id INTEGER NOT NULL,
            aqi INTEGER,
            pm25 REAL,
            pm10 REAL,
            co REAL,
            no2 REAL,
            so2 REAL,
            o3 REAL,
            timestamp INTEGER,
            FOREIGN KEY(city_id) REFERENCES cities(id)
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS health_data (
            id INTEGER PRIMARY KEY,
            state_id INTEGER,
            asthma_rate REAL,
            copd_rate REAL,
            year INTEGER,
            FOREIGN KEY(state_id) REFERENCES states(id)
        )
    '''
    )

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY,
            city_id INTEGER NOT NULL,
            temperature REAL,
            humidity REAL,
            wind_speed REAL,
            pressure REAL,
            weather_condition_id INTEGER,
            timestamp INTEGER,
            FOREIGN KEY(city_id) REFERENCES cities(id),
            FOREIGN KEY(weather_condition_id) REFERENCES weather_conditions(id)
        )
    '''
    )

    conn.commit()
    conn.close()
