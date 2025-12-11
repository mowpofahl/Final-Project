import sqlite3
from pathlib import Path

import pandas as pd

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"


def load_health_dataframe():
    """
    Load all health records joined with county names.
    """
    conn = sqlite3.connect(DB_FILE)
    query = '''
        SELECT
            c.name AS county,
            h.year,
            h.gender,
            h.asthma_rate,
            h.lower_ci,
            h.upper_ci,
            h.visits
        FROM health_data h
        JOIN counties c ON c.id = h.county_id
        WHERE h.asthma_rate IS NOT NULL
        ORDER BY c.name, h.year, h.gender
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    if not df.empty:
        # The state site uses 0 as a placeholder for suppressed values, so drop those upfront.
        df = df[df["asthma_rate"].notna() & (df["asthma_rate"] > 0)]
    return df


def _ensure_df(df):
    return df if df is not None else load_health_dataframe()


def summarize_county_profiles(df=None, verbose=True):
    df = _ensure_df(df)
    if df.empty:
        if verbose:
            print("No health data available for county summaries.")
        return pd.DataFrame(columns=["county", "avg_rate", "median_rate", "avg_visits", "records"])

    # Quick county snapshot so we can shout out where asthma is consistently high.
    agg = (
        df.groupby("county")
        .agg(
            avg_rate=("asthma_rate", "mean"),
            median_rate=("asthma_rate", "median"),
            avg_visits=("visits", "mean"),
            records=("asthma_rate", "count"),
        )
        .reset_index()
        .sort_values("avg_rate", ascending=False)
    )

    if verbose:
        print("Average asthma rates by county (top 5):")
        for _, row in agg.head(5).iterrows():
            print(f"{row['county']}: {row['avg_rate']:.2f} (median {row['median_rate']:.2f}, avg visits {row['avg_visits']:.0f})")
    return agg


def compute_yoy_changes(df=None):
    df = _ensure_df(df)
    if df.empty:
        return pd.DataFrame(columns=["county", "gender", "year", "yoy_change"])

    # Sort so diff() actually compares each county/gender year to the prior year.
    df = df.sort_values(["county", "gender", "year"])
    df["yoy_change"] = df.groupby(["county", "gender"])["asthma_rate"].diff()
    changes = df.dropna(subset=["yoy_change"])[["county", "gender", "year", "yoy_change"]]
    return changes


def summarize_asthma_trends(df=None, verbose=True):
    df = _ensure_df(df)
    if df.empty:
        if verbose:
            print("No health data available for trend summaries.")
        return pd.DataFrame(columns=["county", "gender", "trend_per_year", "start_year", "end_year"])

    results = []
    for (county, gender), group in df.groupby(["county", "gender"]):
        # Treat each county/gender like a mini time series and estimate a slope.
        group = group.sort_values("year")
        years = group["year"].unique()
        if len(years) < 2:
            continue
        span = years[-1] - years[0]
        if span == 0:
            continue
        start_rate = group[group["year"] == years[0]]["asthma_rate"].mean()
        end_rate = group[group["year"] == years[-1]]["asthma_rate"].mean()
        trend = (end_rate - start_rate) / span
        results.append(
            {
                "county": county,
                "gender": gender,
                "trend_per_year": trend,
                "start_year": int(years[0]),
                "end_year": int(years[-1]),
            }
        )

    trend_df = pd.DataFrame(results)
    if trend_df.empty:
        if verbose:
            print("Insufficient multi-year coverage for trend summaries.")
        return trend_df

    if verbose:
        rising = trend_df.sort_values("trend_per_year", ascending=False).head(5)
        falling = trend_df.sort_values("trend_per_year").head(5)
        print("Fastest rising asthma rates (per year):")
        for _, row in rising.iterrows():
            print(f"{row['county']} {row['gender']}: +{row['trend_per_year']:.2f} from {row['start_year']} to {row['end_year']}")
        print("Fastest declining asthma rates (per year):")
        for _, row in falling.iterrows():
            print(f"{row['county']} {row['gender']}: {row['trend_per_year']:.2f} from {row['start_year']} to {row['end_year']}")
    return trend_df


def summarize_gender_patterns(df=None, verbose=True):
    df = _ensure_df(df)
    if df.empty:
        if verbose:
            print("No health data available for gender patterns.")
        return pd.DataFrame(columns=["county", "gender", "avg_rate"])

    pivot = (
        df.groupby(["county", "gender"])["asthma_rate"]
        .mean()
        .reset_index()
    )
    if verbose:
        spread = pivot.pivot(index="county", columns="gender", values="asthma_rate").dropna()
        if not spread.empty:
            spread["gap"] = spread.max(axis=1) - spread.min(axis=1)
            spread = spread.sort_values("gap", ascending=False)
            print("Largest average gender gaps in asthma rates:")
            for county, row in spread.head(5).iterrows():
                print(f"{county}: gap {row['gap']:.2f}")
    return pivot


def summarize_visits_rate_relationship(df=None, verbose=True):
    df = _ensure_df(df)
    if df.empty:
        if verbose:
            print("No health data available for visits vs rate analysis.")
        return pd.DataFrame(columns=["county", "corr", "records"])

    results = []
    for county, group in df.groupby("county"):
        if group["visits"].nunique() < 2 or group["asthma_rate"].nunique() < 2 or len(group) < 4:
            continue
        # Simple Pearson is fine here since we're just checking if busy counties trend higher.
        corr = group["visits"].corr(group["asthma_rate"])
        if pd.notna(corr):
            results.append({"county": county, "corr": corr, "records": len(group)})

    if not results:
        if verbose:
            print("Insufficient variability for visits vs rate correlations.")
        return pd.DataFrame(columns=["county", "corr", "records"])

    corr_df = pd.DataFrame(results).sort_values("corr", ascending=False)
    if verbose:
        print("Visits vs asthma rate correlations:")
        for _, row in corr_df.head(5).iterrows():
            print(f"{row['county']}: corr={row['corr']:.2f} ({row['records']} records)")
    return corr_df


def summarize_yoy_changes(df=None, verbose=True):
    yoy = compute_yoy_changes(df)
    if yoy.empty:
        if verbose:
            print("Insufficient data for year-over-year change analysis.")
        return yoy

    if verbose:
        peaks = yoy.sort_values("yoy_change", ascending=False).head(5)
        troughs = yoy.sort_values("yoy_change").head(5)
        print("Largest year-over-year increases:")
        for _, row in peaks.iterrows():
            print(f"{row['county']} {row['gender']} ({int(row['year'])}): +{row['yoy_change']:.2f}")
        print("Largest year-over-year decreases:")
        for _, row in troughs.iterrows():
            print(f"{row['county']} {row['gender']} ({int(row['year'])}): {row['yoy_change']:.2f}")
    return yoy
