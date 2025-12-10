import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from calculations.health_data_correlation import calculate_health_data_correlation
from calculations.health_insights import compute_yoy_changes, load_health_dataframe
from calculations.pollution_weather import calculate_pollution_weather

DB_FILE = Path(__file__).resolve().parents[1] / "project.db"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "visualizations" / "output"

sns.set_theme(style="whitegrid")


def _fetch_rows(query, params=()):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows


def _fetch_dataframe(query, columns, params=()):
    rows = _fetch_rows(query, params)
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def _load_pm_health_dataframe():
    return _fetch_dataframe(
        '''
        WITH pm AS (
            SELECT
                county_id,
                AVG(pm25) AS avg_pm25
            FROM air_quality
            GROUP BY county_id
        )
        SELECT
            c.name AS county,
            pm.avg_pm25,
            h.asthma_rate,
            h.year AS health_year
        FROM pm
        JOIN counties c ON c.id = pm.county_id
        LEFT JOIN (
            SELECT county_id, year, asthma_rate
            FROM health_data
            WHERE (county_id, year) IN (
                SELECT county_id, MAX(year) FROM health_data GROUP BY county_id
            )
        ) AS h ON h.county_id = pm.county_id
    ''',
        columns=["county", "avg_pm25", "asthma_rate", "health_year"],
    )


def _load_pm_time_series():
    return _fetch_dataframe(
        '''
        SELECT c.name AS county, a.timestamp, a.pm25
        FROM air_quality a
        JOIN counties c ON c.id = a.county_id
        ORDER BY c.name, a.timestamp
    ''',
        columns=["county", "timestamp", "pm25"],
    )


def _format_timestamp(ts):
    if ts is None:
        return ""
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M')


def _print_table(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("No data available.")
        return
    widths = []
    for idx, header in enumerate(headers):
        max_width = len(header)
        for row in rows:
            max_width = max(max_width, len(str(row[idx])))
        widths.append(max_width)
    header_line = " | ".join(f"{header:<{widths[i]}}" for i, header in enumerate(headers))
    divider = "-+-".join("-" * width for width in widths)
    print(header_line)
    print(divider)
    for row in rows:
        print(" | ".join(f"{str(value):<{widths[i]}}" for i, value in enumerate(row)))


def display_recent_batches():
    """
    Print the 25 most recent rows pulled from each source table.
    """
    queries = [
        (
            "Air Quality - Latest 25",
            '''
            SELECT aq.id, c.name, _ts, aq.aqi, aq.pm25, aq.pm10
            FROM (
                SELECT id, county_id, aqi, pm25, pm10, timestamp AS _ts
                FROM air_quality
                ORDER BY id DESC
                LIMIT 25
            ) AS aq
            JOIN counties c ON c.id = aq.county_id
            ORDER BY aq.id DESC
        ''',
            ("ID", "County", "Timestamp (UTC)", "AQI", "PM2.5", "PM10"),
            [2],
        ),
        (
            "Weather - Latest 25",
            '''
            SELECT w.id, c.name, w.timestamp, w.temperature, w.humidity, w.wind_speed
            FROM (
                SELECT id, county_id, temperature, humidity, wind_speed, timestamp
                FROM weather_data
                ORDER BY id DESC
                LIMIT 25
            ) AS w
            JOIN counties c ON c.id = w.county_id
            ORDER BY w.id DESC
        ''',
            ("ID", "County", "Timestamp (UTC)", "Temp (F)", "Humidity", "Wind (mph)"),
            [2],
        ),
        (
            "Health Data - Latest 25",
            '''
            SELECT h.id, c.name, h.year, h.gender, h.asthma_rate, h.visits
            FROM (
                SELECT *
                FROM health_data
                ORDER BY id DESC
                LIMIT 25
            ) AS h
            JOIN counties c ON c.id = h.county_id
            ORDER BY h.id DESC
        ''',
            ("ID", "County", "Year", "Gender", "Asthma Rate", "Visits"),
            [],
        ),
    ]
    for title, query, headers, ts_indexes in queries:
        rows = _fetch_rows(query)
        if ts_indexes:
            formatted_rows = []
            for row in rows:
                row = list(row)
                for idx in ts_indexes:
                    row[idx] = _format_timestamp(row[idx])
                formatted_rows.append(tuple(row))
            rows = formatted_rows
        _print_table(title, headers, rows)


def _plot_asthma_trends(health_df):
    if health_df.empty:
        print("Skipping asthma trend plot (no health data).")
        return

    coverage = (
        health_df.groupby("county")["year"]
        .nunique()
        .sort_values(ascending=False)
    )
    top_counties = coverage.head(4).index.tolist()
    if not top_counties:
        print("Skipping asthma trend plot (insufficient county coverage).")
        return

    fig, axes = plt.subplots(len(top_counties), 1, figsize=(9, 4 * len(top_counties)), sharex=True)
    if len(top_counties) == 1:
        axes = [axes]

    for county, ax in zip(top_counties, axes):
        subset = health_df[health_df["county"] == county].sort_values("year")
        sns.lineplot(data=subset, x="year", y="asthma_rate", hue="gender", marker="o", ax=ax)
        ax.set_title(f"{county} asthma trends")
        ax.set_ylabel("Asthma rate")
        ax.legend(title="Gender", loc="upper left")
        ax.grid(True, axis="y", alpha=0.3)
    axes[-1].set_xlabel("Year")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "asthma_trends.png")
    plt.close(fig)


def _plot_county_profiles(health_df):
    if health_df.empty:
        print("Skipping county profile plot (no health data).")
        return

    agg = (
        health_df.groupby("county")
        .agg(avg_rate=("asthma_rate", "mean"), avg_visits=("visits", "mean"))
        .reset_index()
        .sort_values("avg_rate", ascending=False)
    )

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    sns.barplot(data=agg, x="county", y="avg_rate", ax=axes[0])
    axes[0].set_ylabel("Avg asthma rate")
    axes[0].set_title("Average asthma rate per county")
    sns.barplot(data=agg, x="county", y="avg_visits", ax=axes[1])
    axes[1].set_ylabel("Avg annual visits")
    axes[1].set_title("Average asthma visit volume per county")
    for ax in axes:
        ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "county_profiles.png")
    plt.close(fig)


def _plot_gender_heatmap(health_df):
    if health_df.empty:
        print("Skipping gender heatmap (no health data).")
        return

    pivot = health_df.pivot_table(index="county", columns="gender", values="asthma_rate", aggfunc="mean")
    pivot = pivot.dropna(how="all")
    if pivot.empty:
        print("Skipping gender heatmap (insufficient gender coverage).")
        return

    fig, ax = plt.subplots(figsize=(8, max(4, len(pivot) * 0.4)))
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="RdYlBu_r", ax=ax)
    ax.set_title("Average asthma rate by county and gender")
    ax.set_xlabel("Gender")
    ax.set_ylabel("County")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "gender_heatmap.png")
    plt.close(fig)


def _plot_visits_vs_rate(health_df):
    if health_df.empty:
        print("Skipping visits vs rate scatter (no health data).")
        return

    subset = health_df.dropna(subset=["visits", "asthma_rate"])
    if subset.empty:
        print("Skipping visits vs rate scatter (missing required fields).")
        return

    fig, ax = plt.subplots(figsize=(9, 6))
    sns.scatterplot(
        data=subset,
        x="visits",
        y="asthma_rate",
        hue="county",
        style="gender",
        ax=ax,
    )
    ax.set_title("Asthma visits vs rate per county-year")
    ax.set_xlabel("Annual visits")
    ax.set_ylabel("Asthma rate")
    ax.legend(loc="best", fontsize="small")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "visits_vs_rate.png")
    plt.close(fig)


def _plot_pm_vs_asthma(pm_df):
    if pm_df.empty:
        print("Skipping PM vs asthma plot (no overlapping data).")
        return

    pm_df = pm_df.dropna(subset=["avg_pm25", "asthma_rate"])
    pm_df = pm_df[pm_df["asthma_rate"] > 0]
    if pm_df.empty:
        print("Skipping PM vs asthma plot (no overlapping data).")
        return

    fig, ax = plt.subplots(figsize=(9, 6))
    sns.scatterplot(data=pm_df, x="avg_pm25", y="asthma_rate", hue="county", ax=ax)
    ax.set_title("Average PM2.5 vs asthma rate by county-year")
    ax.set_xlabel("Avg PM2.5 (µg/m³)")
    ax.set_ylabel("Asthma rate")
    ax.legend(loc="best", fontsize="small")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pm_vs_asthma.png")
    plt.close(fig)


def _plot_pm_trends(pm_series_df):
    if pm_series_df.empty:
        print("Skipping PM trend plot (no PM data).")
        return

    pm_series_df = pm_series_df.dropna(subset=["pm25"])
    if pm_series_df.empty:
        print("Skipping PM trend plot (no PM data).")
        return

    pm_series_df["datetime"] = pd.to_datetime(pm_series_df["timestamp"], unit="s")
    coverage = (
        pm_series_df.groupby("county")["datetime"]
        .nunique()
        .sort_values(ascending=False)
    )
    top_counties = coverage.head(3).index.tolist()
    if not top_counties:
        print("Skipping PM trend plot (insufficient county coverage).")
        return

    fig, axes = plt.subplots(len(top_counties), 1, figsize=(9, 4 * len(top_counties)), sharex=True)
    if len(top_counties) == 1:
        axes = [axes]

    for county, ax in zip(top_counties, axes):
        subset = pm_series_df[pm_series_df["county"] == county].sort_values("datetime")
        ax.plot(subset["datetime"], subset["pm25"], color="tab:green", marker="o")
        ax.set_ylabel("PM2.5 (µg/m³)")
        ax.set_title(f"{county}: PM2.5 readings over time")
        ax.grid(True, axis="y", alpha=0.3)
    axes[-1].set_xlabel("Timestamp")

    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pm_trends.png")
    plt.close(fig)


def _plot_pm_seasonal_heatmap():
    rows = _fetch_rows(
        '''
        SELECT
            c.name,
            CAST(strftime('%m', datetime(a.timestamp, 'unixepoch')) AS INTEGER) AS month,
            a.pm25
        FROM air_quality a
        JOIN counties c ON c.id = a.county_id
    '''
    )
    if not rows:
        print("Skipping PM seasonal heatmap (no air quality data).")
        return

    season_map = {
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
    seasonal = defaultdict(lambda: defaultdict(list))
    for county, month, pm25 in rows:
        season = season_map.get(month)
        if season and pm25 is not None:
            seasonal[county][season].append(pm25)

    data = []
    for county, seasons in seasonal.items():
        averages = {season: sum(values) / len(values) for season, values in seasons.items()}
        averages["county"] = county
        data.append(averages)

    if not data:
        print("Skipping PM seasonal heatmap (insufficient coverage).")
        return

    df = pd.DataFrame(data).set_index("county")
    seasons = ["Winter", "Spring", "Summer", "Fall"]
    df = df.reindex(columns=seasons, fill_value=float("nan")).fillna(0.0)

    fig, ax = plt.subplots(figsize=(8, max(4, len(df) * 0.4)))
    sns.heatmap(df, annot=True, fmt=".2f", cmap="YlOrRd", ax=ax)
    ax.set_title("Average PM2.5 by season and county")
    ax.set_xlabel("Season")
    ax.set_ylabel("County")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pm_seasonal_heatmap.png")
    plt.close(fig)


def _plot_pm_weather_correlations():
    data = calculate_pollution_weather(verbose=False)
    if not data:
        print("Skipping PM-weather correlation plot (no paired samples).")
        return

    df = pd.DataFrame(data)
    if df.empty:
        print("Skipping PM-weather correlation plot (no correlated counties).")
        return
    melted = df.melt(
        id_vars=["county"],
        value_vars=["temp_corr", "humidity_corr", "wind_corr"],
        var_name="metric",
        value_name="correlation",
    )
    melted["correlation"] = melted["correlation"].fillna(0.0)
    metric_labels = {
        "temp_corr": "Temperature",
        "humidity_corr": "Humidity",
        "wind_corr": "Wind Speed",
    }
    melted["metric"] = melted["metric"].map(metric_labels)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=melted, x="county", y="correlation", hue="metric", ax=ax)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("PM2.5 correlation with weather metrics")
    ax.set_xlabel("County")
    ax.set_ylabel("Pearson correlation")
    ax.legend(title="Metric")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pm_weather_correlations.png")
    plt.close(fig)


def _plot_pm_forecast():
    forecasts = calculate_pollution_forecasting(verbose=False)
    if not forecasts:
        print("Skipping PM forecast chart (insufficient PM history).")
        return

    df = pd.DataFrame(forecasts)
    df = df.sort_values("forecast_pm25", ascending=False).head(8)
    melted = df.melt(
        id_vars=["county"],
        value_vars=["forecast_pm25", "latest_pm25"],
        var_name="metric",
        value_name="value",
    )
    metric_labels = {"forecast_pm25": "Forecast", "latest_pm25": "Latest"}
    melted["metric"] = melted["metric"].map(metric_labels)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=melted, x="county", y="value", hue="metric", ax=ax)
    ax.set_title("Projected vs latest PM2.5 (3-sample moving average)")
    ax.set_xlabel("County")
    ax.set_ylabel("PM2.5 (µg/m³)")
    ax.legend(title="")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pm_forecast.png")
    plt.close(fig)


def _plot_health_similarity_heatmap():
    correlations = calculate_health_data_correlation(verbose=False)
    if not correlations:
        print("Skipping health similarity heatmap (insufficient overlap).")
        return

    counts = Counter()
    for entry in correlations:
        counts[entry['county_a']] += 1
        counts[entry['county_b']] += 1

    top_counties = [county for county, _ in counts.most_common(6)]
    if not top_counties:
        print("Skipping health similarity heatmap (no frequent counties).")
        return

    matrix = pd.DataFrame(1.0, index=top_counties, columns=top_counties)
    lookup = {
        (entry['county_a'], entry['county_b']): entry['corr']
        for entry in correlations
    }
    for left in top_counties:
        for right in top_counties:
            if left == right:
                continue
            value = lookup.get((left, right)) or lookup.get((right, left))
            if value is not None:
                matrix.loc[left, right] = value
            else:
                matrix.loc[left, right] = 0

    fig, ax = plt.subplots(figsize=(7, 6))
    sns.heatmap(matrix, annot=True, vmin=-1, vmax=1, cmap="coolwarm", ax=ax)
    ax.set_title("County Asthma Rate Trajectory Similarity")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "health_similarity_heatmap.png")
    plt.close(fig)


def _plot_forecast_bars():
    forecasts = calculate_pollution_forecasting(verbose=False)
    if not forecasts:
        print("Skipping forecast chart (no AQI history).")
        return

    df = pd.DataFrame(forecasts)
    df = df.sort_values("forecast_aqi", ascending=False).head(8)
    melted = df.melt(
        id_vars=["county"],
        value_vars=["forecast_aqi", "latest_aqi"],
        var_name="metric",
        value_name="value",
    )
    metric_labels = {"forecast_aqi": "Forecast", "latest_aqi": "Latest"}
    melted["metric"] = melted["metric"].map(metric_labels)

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=melted, x="county", y="value", hue="metric", ax=ax)
    ax.set_title("Projected vs Latest AQI (3-point moving average)")
    ax.set_xlabel("County")
    ax.set_ylabel("AQI")
    ax.legend(title="")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "aqi_forecast.png")
    plt.close(fig)


def generate_visualizations():
    """
    Generates health-focused visualizations plus the tabular snapshot of the latest 25 ingested items.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    display_recent_batches()
    health_df = load_health_dataframe()
    pm_health_df = _load_pm_health_dataframe()
    _plot_asthma_trends(health_df)
    _plot_gender_heatmap(health_df)
    _plot_visits_vs_rate(health_df)
    _plot_pm_vs_asthma(pm_health_df)
    _plot_pm_weather_correlations()
