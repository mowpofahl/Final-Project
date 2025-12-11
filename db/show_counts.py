"""
Utility script to summarize how many rows live in each API-backed table.

Run `python db/show_counts.py` after `python main.py` to verify how much data
has been captured. The script prints counts for air_quality, weather_data, and
health_data so you can show the grader you have ≥100 rows per source.
"""

import sqlite3
from pathlib import Path

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def _ensure_db():
    if DB_FILE.exists():
        return
    raise SystemExit(
        f"Database {DB_FILE} does not exist yet. Run `python main.py` first so data can be ingested."
    )


def show_counts():
    _ensure_db()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    tables = (
        ('air_quality', 'Air quality measurements'),
        ('weather_data', 'Weather snapshots'),
        ('health_data', 'Health/asthma records'),
    )
    for table, label in tables:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        total = cursor.fetchone()[0]
        print(f"{label}: {total} rows")
    conn.close()
    print(f"\nDatabase file: {DB_FILE}")


if __name__ == "__main__":
    show_counts()
