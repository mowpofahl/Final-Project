import sqlite3
from pathlib import Path
from collections import defaultdict

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def _trend(values):
    if len(values) < 2:
        return 0.0, 0.0
    xs = [item[0] for item in values]
    ys = [item[1] for item in values]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return 0.0, 0.0
    slope = num / den
    intercept = mean_y - slope * mean_x
    return slope, intercept


def calculate_pollution_health_trend(verbose=True):
    """
    Track linear trend slopes for AQI and asthma rates by county and year.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        WITH aq AS (
            SELECT
                county_id,
                CAST(strftime('%Y', datetime(observed_at, 'unixepoch')) AS INTEGER) AS year,
                AVG(aqi) AS avg_aqi
            FROM air_quality
            GROUP BY county_id, year
        )
        SELECT c.name, aq.year, aq.avg_aqi, h.asthma_rate
        FROM aq
        JOIN health_data h ON h.county_id = aq.county_id AND h.year = aq.year
        JOIN counties c ON c.id = aq.county_id
        ORDER BY c.name, aq.year
    '''
    )
    rows = cursor.fetchall()
    conn.close()

    series = defaultdict(list)
    for county, year, avg_aqi, asthma in rows:
        if avg_aqi is None or asthma is None:
            continue
        series[county].append((year, avg_aqi, asthma))

    trends = []
    for county, data in series.items():
        if len(data) < 2:
            continue
        aqi_points = [(year, avg_aqi) for year, avg_aqi, _ in data]
        asthma_points = [(year, asthma) for year, _, asthma in data]
        aqi_slope, _ = _trend(aqi_points)
        asthma_slope, _ = _trend(asthma_points)
        trends.append(
            {
                'county': county,
                'years': len(data),
                'aqi_slope': aqi_slope,
                'asthma_slope': asthma_slope,
            }
        )

    trends.sort(key=lambda entry: entry['asthma_slope'], reverse=True)
    if verbose:
        print("Top upward asthma trends:")
        for entry in trends[:5]:
            print(
                f"{entry['county']}: asthma slope {entry['asthma_slope']:.3f} "
                f"(AQI slope {entry['aqi_slope']:.3f}, {entry['years']} years)"
            )
    return trends
