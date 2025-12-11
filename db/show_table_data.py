"""
Print the full contents of each API-backed table so you can prove you captured
every record. By default this dumps all three tables, but you can pass a table
name (air_quality, weather_data, health_data) and/or --limit to control output.
"""

import argparse
import sqlite3
from pathlib import Path

try:
    from db_setup import create_db
except ModuleNotFoundError:
    from db.db_setup import create_db  # type: ignore

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"

TABLES = {
    'air_quality': {
        'label': 'Air quality measurements',
        'headers': [
            'id',
            'county',
            'state',
            'aqi',
            'pm25',
            'pm10',
            'co',
            'no2',
            'so2',
            'o3',
            'observed_at',
            'ingested_at',
        ],
        'query': '''
            SELECT
                aq.id,
                c.name,
                c.state,
                aq.aqi,
                aq.pm25,
                aq.pm10,
                aq.co,
                aq.no2,
                aq.so2,
                aq.o3,
                aq.observed_at,
                aq.timestamp
            FROM air_quality aq
            JOIN counties c ON c.id = aq.county_id
            ORDER BY aq.id
        ''',
    },
    'weather_data': {
        'label': 'Weather snapshots',
        'headers': [
            'id',
            'county',
            'state',
            'temp(F)',
            'humidity',
            'wind_speed',
            'pressure',
            'description',
            'observed_at',
            'ingested_at',
        ],
        'query': '''
            SELECT
                w.id,
                c.name,
                c.state,
                w.temperature,
                w.humidity,
                w.wind_speed,
                w.pressure,
                w.description,
                w.observed_at,
                w.timestamp
            FROM weather_data w
            JOIN counties c ON c.id = w.county_id
            ORDER BY w.id
        ''',
    },
    'health_data': {
        'label': 'Health/asthma records',
        'headers': ['id', 'county', 'state', 'gender', 'year', 'asthma_rate', 'lower_ci', 'upper_ci', 'visits'],
        'query': '''
            SELECT
                h.id,
                c.name,
                c.state,
                h.gender,
                h.year,
                h.asthma_rate,
                h.lower_ci,
                h.upper_ci,
                h.visits
            FROM health_data h
            JOIN counties c ON c.id = h.county_id
            ORDER BY h.id
        ''',
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description="Dump table data stored in project.db.")
    parser.add_argument(
        'table',
        nargs='?',
        choices=TABLES.keys(),
        help="Optional table to display (defaults to all tables).",
    )
    parser.add_argument(
        '--limit',
        type=int,
        help="Limit output rows per table (leave unset to show every row).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not DB_FILE.exists():
        raise SystemExit(f"Database {DB_FILE} not found. Run `python main.py` to populate it first.")

    # Ensure any schema migrations (like the observed_at column) have been applied.
    create_db()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    targets = [args.table] if args.table else TABLES.keys()
    for table_name in targets:
        config = TABLES[table_name]
        print(f"\n{config['label']} ({table_name})")
        print("-" * 80)
        headers = config['headers']
        header_line = " | ".join(headers)
        print(header_line)
        print("-" * len(header_line))

        sql = config['query']
        if args.limit:
            sql += f" LIMIT {args.limit}"
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            formatted = " | ".join("" if value is None else str(value) for value in row)
            print(formatted)
        if not rows:
            print("(no rows)")
        print(f"-- {len(rows)} row(s) shown")
    cursor.close()
    conn.close()
    print(f"\nDatabase file: {DB_FILE}")


if __name__ == "__main__":
    main()
