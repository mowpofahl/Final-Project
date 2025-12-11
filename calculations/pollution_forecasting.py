import sqlite3
from pathlib import Path
from collections import defaultdict
from statistics import mean

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def calculate_pollution_forecasting(verbose=True):
    """
    Produce a simple moving-average PM2.5 forecast per county and compare against the latest asthma rate.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT c.name, a.observed_at, a.pm25
        FROM air_quality a
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, a.observed_at
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
    for county, observed_at, pm25 in pollution_rows:
        if pm25 is None:
            continue
        series[county].append((observed_at, pm25))

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
                'forecast_pm25': forecast,
                'latest_pm25': latest_value,
                'delta': delta,
                'latest_asthma_rate': health_info[1] if health_info else None,
                'asthma_year': health_info[0] if health_info else None,
            }
        )

    forecasts.sort(key=lambda entry: entry['forecast_pm25'], reverse=True)
    if verbose:
        print("Projected PM2.5 levels (3-sample moving average):")
        for entry in forecasts[:5]:
            print(
                f"{entry['county']}: forecast PM2.5 {entry['forecast_pm25']:.2f} "
                f"(last {entry['latest_pm25']:.2f}, Δ {entry['delta']:.2f}, "
                f"asthma {entry['latest_asthma_rate'] or 'n/a'} in {entry['asthma_year'] or 'n/a'})"
            )
    return forecasts
