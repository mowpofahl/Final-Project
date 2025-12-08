import sqlite3
from collections import defaultdict

from scipy.stats import pearsonr

DB_FILE = "project.db"
MAX_TIME_DIFF_SECONDS = 3600  # pair measurements captured within one hour


def calculate_pollution_weather(verbose=True):
    """
    Correlate AQI with temperature, humidity, and wind speed for each Colorado county.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        f'''
        SELECT
            c.name,
            a.timestamp,
            a.aqi,
            w.temperature,
            w.humidity,
            w.wind_speed,
            w.pressure
        FROM air_quality a
        JOIN weather_data w
            ON a.county_id = w.county_id
           AND ABS(a.timestamp - w.timestamp) <= {MAX_TIME_DIFF_SECONDS}
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, a.timestamp
    '''
    )
    rows = cursor.fetchall()
    conn.close()

    series = defaultdict(lambda: {'aqi': [], 'temp': [], 'humidity': [], 'wind': []})
    for county, _ts, aqi, temp, humidity, wind_speed, _pressure in rows:
        if None in (aqi, temp, humidity, wind_speed):
            continue
        series[county]['aqi'].append(aqi)
        series[county]['temp'].append(temp)
        series[county]['humidity'].append(humidity)
        series[county]['wind'].append(wind_speed)

    correlations = []
    for county, metrics in series.items():
        if len(metrics['aqi']) < 5:
            continue

        def correlate(values):
            try:
                corr, _ = pearsonr(metrics['aqi'], values)
                return corr
            except Exception:
                return float('nan')

        correlations.append(
            {
                'county': county,
                'observations': len(metrics['aqi']),
                'temp_corr': correlate(metrics['temp']),
                'humidity_corr': correlate(metrics['humidity']),
                'wind_corr': correlate(metrics['wind']),
            }
        )

    correlations.sort(key=lambda entry: abs(entry['temp_corr']) if entry['temp_corr'] == entry['temp_corr'] else 0, reverse=True)
    if verbose:
        print("Pollution-weather correlations (AQI vs metric):")
        for entry in correlations[:5]:
            print(
                f"{entry['county']} - temp {entry['temp_corr']:.2f}, "
                f"humidity {entry['humidity_corr']:.2f}, wind {entry['wind_corr']:.2f} "
                f"({entry['observations']} samples)"
            )
    return correlations
