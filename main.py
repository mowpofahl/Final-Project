from api.fetch_air_quality import fetch_air_quality_data
from api.fetch_health_data import fetch_health_data
from api.fetch_weather_data import fetch_weather_data
from db.db_operations import store_data_in_db
from calculations.health_data_correlation import calculate_health_data_correlation
from calculations.health_insights import (
    summarize_asthma_trends,
    summarize_county_profiles,
    summarize_gender_patterns,
    summarize_visits_rate_relationship,
    summarize_yoy_changes,
)
from visualizations.visualize import generate_visualizations


def main():
    # Ensure the database schema exists before inserting data
    store_data_in_db()

    # Fetch data from APIs
    fetch_air_quality_data()   # Fetch air quality data
    fetch_health_data()        # Fetch health data
    fetch_weather_data()       # Fetch weather data

    # Perform calculations
    summarize_county_profiles()             # County-level averages
    summarize_asthma_trends()               # Trend direction per county/gender
    summarize_gender_patterns()             # Gender disparities
    summarize_visits_rate_relationship()    # Visits vs asthma rates
    summarize_yoy_changes()                 # YoY swings
    calculate_health_data_correlation()     # Cross-county trajectory similarity

    # Generate visualizations
    generate_visualizations()


if __name__ == "__main__":
    main()
