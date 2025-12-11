import sqlite3
from pathlib import Path
from collections import defaultdict

from scipy.stats import pearsonr

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"
MAX_TIME_DIFF_SECONDS = 3600  # pair measurements captured within one hour


def calculate_pollution_weather(verbose=True):
    """
    Correlate PM2.5 with temperature, humidity, and wind speed for each Colorado county.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        f'''
        SELECT
            c.name,
            a.observed_at,
            a.pm25,
            w.temperature,
            w.humidity,
            w.wind_speed,
            w.pressure
        FROM air_quality a
        JOIN weather_data w
            ON a.county_id = w.county_id
           AND ABS(a.observed_at - w.observed_at) <= {MAX_TIME_DIFF_SECONDS}
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, a.observed_at
    '''
    )
    rows = cursor.fetchall()
    conn.close()

    series = defaultdict(lambda: {'pm25': [], 'temp': [], 'humidity': [], 'wind': []})
    for county, _ts, pm25, temp, humidity, wind_speed, _pressure in rows:
        if None in (pm25, temp, humidity, wind_speed):
            continue
        series[county]['pm25'].append(pm25)
        series[county]['temp'].append(temp)
        series[county]['humidity'].append(humidity)
        series[county]['wind'].append(wind_speed)

    correlations = []
    for county, metrics in series.items():
        if len(metrics['pm25']) < 2:
            continue

        def correlate(values):
            pm_values = metrics['pm25']
            if len(set(pm_values)) <= 1 or len(set(values)) <= 1:
                return 0.0
            try:
                corr, _ = pearsonr(pm_values, values)
                return corr
            except Exception:
                return 0.0

        correlations.append(
            {
                'county': county,
                'observations': len(metrics['pm25']),
                'temp_corr': correlate(metrics['temp']),
                'humidity_corr': correlate(metrics['humidity']),
                'wind_corr': correlate(metrics['wind']),
            }
        )

    correlations.sort(key=lambda entry: abs(entry['temp_corr']) if entry['temp_corr'] == entry['temp_corr'] else 0, reverse=True)
    if verbose:
        print("Pollution-weather correlations (PM2.5 vs metric):")
        for entry in correlations[:5]:
            print(
                f"{entry['county']} - temp {entry['temp_corr']:.2f}, "
                f"humidity {entry['humidity_corr']:.2f}, wind {entry['wind_corr']:.2f} "
                f"({entry['observations']} samples)"
            )
    return correlations
