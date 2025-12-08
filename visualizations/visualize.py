import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from calculations.health_data_correlation import calculate_health_data_correlation
from calculations.health_insights import compute_yoy_changes, load_health_dataframe

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


def _plot_yoy_change_lollipops(health_df):
    yoy = compute_yoy_changes(health_df)
    if yoy.empty:
        print("Skipping year-over-year change plot (insufficient data).")
        return

    yoy["year"] = yoy["year"].astype(int)
    yoy["abs_change"] = yoy["yoy_change"].abs()
    top_counties = (
        yoy.groupby("county")["abs_change"]
        .max()
        .sort_values(ascending=False)
        .head(5)
        .index.tolist()
    )
    subset = yoy[yoy["county"].isin(top_counties)]
    if subset.empty:
        print("Skipping year-over-year change plot (no qualifying counties).")
        return

    fig, axes = plt.subplots(len(top_counties), 1, figsize=(10, 3.5 * len(top_counties)), sharex=True)
    if len(top_counties) == 1:
        axes = [axes]
    palette = sns.color_palette("tab10")
    gender_colors = {gender: palette[idx % len(palette)] for idx, gender in enumerate(sorted(subset["gender"].unique()))}

    for county, ax in zip(top_counties, axes):
        county_data = subset[subset["county"] == county].sort_values("year")
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
        for gender, group in county_data.groupby("gender"):
            color = gender_colors[gender]
            ax.vlines(group["year"], 0, group["yoy_change"], colors=color, linewidth=1.5)
            ax.scatter(group["year"], group["yoy_change"], color=color, s=45, label=gender)
        ax.set_title(f"{county}: YoY asthma rate change")
        ax.set_ylabel("Δ rate")
        ax.grid(True, axis="y", alpha=0.2)
    axes[-1].set_xlabel("Year")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", title="Gender")
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "yoy_change_lollipop.png")
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
    _plot_asthma_trends(health_df)
    _plot_county_profiles(health_df)
    _plot_gender_heatmap(health_df)
    _plot_visits_vs_rate(health_df)
    _plot_yoy_change_lollipops(health_df)
    _plot_health_similarity_heatmap()
