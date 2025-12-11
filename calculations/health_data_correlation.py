import itertools
import sqlite3
from pathlib import Path

from scipy.stats import pearsonr

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def calculate_health_data_correlation(verbose=True):
    """
    Computes correlations between counties based on multi-year asthma rates.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT c.name, year, asthma_rate
        FROM health_data h
        JOIN counties c ON c.id = h.county_id
        WHERE asthma_rate IS NOT NULL
        ORDER BY c.name, year
    '''
    )
    rows = cursor.fetchall()
    conn.close()

    series = {}
    for county, year, rate in rows:
        series.setdefault(county, {})[year] = rate

    correlations = []
    counties = sorted(series.keys())
    for left, right in itertools.combinations(counties, 2):
        left_data = series[left]
        right_data = series[right]
        shared_years = sorted(set(left_data.keys()) & set(right_data.keys()))
        if len(shared_years) < 3:
            continue
        left_values = [left_data[year] for year in shared_years]
        right_values = [right_data[year] for year in shared_years]
        try:
            corr, _ = pearsonr(left_values, right_values)
        except Exception:
            continue
        correlations.append({'county_a': left, 'county_b': right, 'corr': corr, 'years': len(shared_years)})

    correlations.sort(key=lambda entry: abs(entry['corr']), reverse=True)
    if verbose:
        print("Top correlated county asthma trajectories:")
        for entry in correlations[:5]:
            print(
                f"{entry['county_a']} & {entry['county_b']}: corr={entry['corr']:.2f} "
                f"across {entry['years']} shared years"
            )
    return correlations
