import sqlite3
from pathlib import Path
from collections import defaultdict

from scipy.stats import pearsonr

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def calculate_pollution_health(verbose=True):
    """
    Calculate per-county Pearson correlations between annual mean AQI/PM2.5 and asthma rates.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        WITH aq AS (
            SELECT
                county_id,
                CAST(strftime('%Y', datetime(observed_at, 'unixepoch')) AS INTEGER) AS year,
                AVG(aqi) AS avg_aqi,
                AVG(pm25) AS avg_pm25
            FROM air_quality
            GROUP BY county_id, year
        )
        SELECT c.name, aq.year, aq.avg_aqi, aq.avg_pm25, h.asthma_rate
        FROM aq
        JOIN health_data h ON h.county_id = aq.county_id AND h.year = aq.year
        JOIN counties c ON c.id = aq.county_id
        ORDER BY c.name, aq.year
    '''
    )
    rows = cursor.fetchall()
    conn.close()

    by_county = defaultdict(list)
    for county, year, avg_aqi, avg_pm25, asthma_rate in rows:
        if avg_aqi is None or asthma_rate is None:
            continue
        by_county[county].append((avg_aqi, avg_pm25, asthma_rate, year))

    correlations = []
    for county, records in by_county.items():
        if len(records) < 2:
            continue
        aqi_values = [record[0] for record in records]
        asthma_values = [record[2] for record in records]
        pm_values = [record[1] for record in records]
        try:
            aqi_corr, _ = pearsonr(aqi_values, asthma_values)
        except Exception:
            aqi_corr = float('nan')
        try:
            pm_corr, _ = pearsonr(pm_values, asthma_values)
        except Exception:
            pm_corr = float('nan')
        correlations.append(
            {
                'county': county,
                'observations': len(records),
                'aqi_corr': aqi_corr,
                'pm25_corr': pm_corr,
            }
        )

    correlations.sort(key=lambda item: (abs(item['aqi_corr']) if item['aqi_corr'] == item['aqi_corr'] else 0), reverse=True)
    if verbose:
        for entry in correlations[:5]:
            print(
                f"{entry['county']}: AQI↔Asthma corr={entry['aqi_corr']:.2f}, "
                f"PM2.5↔Asthma corr={entry['pm25_corr']:.2f} ({entry['observations']} yrs)"
            )
    return correlations
