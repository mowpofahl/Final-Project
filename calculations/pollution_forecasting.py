import sqlite3
from collections import defaultdict
from statistics import mean

DB_FILE = "project.db"


def calculate_pollution_forecasting(verbose=True):
    """
    Produce a simple moving-average AQI forecast per county and compare against the latest asthma rate.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT c.name, a.timestamp, a.aqi
        FROM air_quality a
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, a.timestamp
    '''
    )
    pollution_rows = cursor.fetchall()

    cursor.execute(
        '''
        SELECT c.name, h.year, h.asthma_rate
        FROM health_data h
        JOIN counties c ON c.id = h.county_id
        WHERE (h.county_id, h.year) IN (
            SELECT county_id, MAX(year) FROM health_data GROUP BY county_id
        )
    '''
    )
    health_rows = cursor.fetchall()
    conn.close()

    latest_health = {county: (year, rate) for county, year, rate in health_rows if rate is not None}

    series = defaultdict(list)
    for county, timestamp, aqi in pollution_rows:
        if aqi is None:
            continue
        series[county].append((timestamp, aqi))

    forecasts = []
    for county, entries in series.items():
        if len(entries) < 3:
            continue
        entries.sort(key=lambda item: item[0])
        values = [item[1] for item in entries]
        window = values[-3:]
        forecast = mean(window)
        latest_value = values[-1]
        delta = forecast - latest_value
        health_info = latest_health.get(county)
        forecasts.append(
            {
                'county': county,
                'forecast_aqi': forecast,
                'latest_aqi': latest_value,
                'delta': delta,
                'latest_asthma_rate': health_info[1] if health_info else None,
                'asthma_year': health_info[0] if health_info else None,
            }
        )

    forecasts.sort(key=lambda entry: entry['forecast_aqi'], reverse=True)
    if verbose:
        print("Projected AQI levels (3-sample moving average):")
        for entry in forecasts[:5]:
            print(
                f"{entry['county']}: forecast AQI {entry['forecast_aqi']:.1f} "
                f"(last {entry['latest_aqi']:.1f}, Δ {entry['delta']:.1f}, "
                f"asthma {entry['latest_asthma_rate'] or 'n/a'} in {entry['asthma_year'] or 'n/a'})"
            )
    return forecasts
