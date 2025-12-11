import sqlite3
from pathlib import Path
from collections import defaultdict

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"

SEASONS = {
    12: "Winter",
    1: "Winter",
    2: "Winter",
    3: "Spring",
    4: "Spring",
    5: "Spring",
    6: "Summer",
    7: "Summer",
    8: "Summer",
    9: "Fall",
    10: "Fall",
    11: "Fall",
}


def calculate_seasonal_trends(verbose=True):
    """
    Determine which season contributes the highest average AQI for each county-year and align it with asthma rates.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT
            c.name,
            CAST(strftime('%Y', datetime(a.observed_at, 'unixepoch')) AS INTEGER) AS year,
            CAST(strftime('%m', datetime(a.observed_at, 'unixepoch')) AS INTEGER) AS month,
            a.aqi
        FROM air_quality a
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, year, month
    '''
    )
    pollution_rows = cursor.fetchall()

    cursor.execute(
        '''
        SELECT c.name, year, asthma_rate
        FROM health_data h
        JOIN counties c ON c.id = h.county_id
    '''
    )
    health_rows = cursor.fetchall()
    conn.close()

    health_lookup = {(county, year): rate for county, year, rate in health_rows if rate is not None}

    seasonal_data = defaultdict(lambda: defaultdict(list))
    for county, year, month, aqi in pollution_rows:
        if aqi is None:
            continue
        season = SEASONS.get(month)
        seasonal_data[(county, year)][season].append(aqi)

    summaries = []
    for key, season_values in seasonal_data.items():
        county, year = key
        asthma_rate = health_lookup.get((county, year))
        averages = {season: sum(values) / len(values) for season, values in season_values.items()}
        if not averages:
            continue
        dominant_season = max(averages.items(), key=lambda item: item[1])
        summaries.append(
            {
                'county': county,
                'year': year,
                'dominant_season': dominant_season[0],
                'avg_aqi': dominant_season[1],
                'asthma_rate': asthma_rate,
            }
        )

    summaries.sort(key=lambda entry: entry['avg_aqi'], reverse=True)
    if verbose:
        print("Seasonal AQI leaders per county-year:")
        for entry in summaries[:10]:
            print(
                f"{entry['county']} {entry['year']}: {entry['dominant_season']} avg AQI {entry['avg_aqi']:.1f} "
                f"(asthma rate {entry['asthma_rate'] or 'n/a'})"
            )
    return summaries
