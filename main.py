from api.fetch_air_quality import fetch_air_quality_data
from api.fetch_health_data import fetch_health_data
from api.fetch_weather_data import fetch_weather_data
from db.db_operations import store_data_in_db
from calculations.pollution_health import calculate_pollution_health
from calculations.pollution_weather import calculate_pollution_weather
from calculations.seasonal_trends import calculate_seasonal_trends
from calculations.pollution_health_trend import calculate_pollution_health_trend
from calculations.health_data_correlation import calculate_health_data_correlation
from calculations.pollution_forecasting import calculate_pollution_forecasting
from visualizations.visualize import generate_visualizations


def main():
    # Ensure the database schema exists before inserting data
    store_data_in_db()

    # Fetch data from APIs
    fetch_air_quality_data()   # Fetch air quality data
    fetch_health_data()        # Fetch health data
    fetch_weather_data()       # Fetch weather data

    # Perform calculations
    calculate_pollution_health()          # Pollution-health correlation
    calculate_pollution_weather()         # Pollution-weather interaction
    calculate_seasonal_trends()           # Seasonal & Regional trends
    calculate_pollution_health_trend()    # Pollution and health trend over time
    calculate_health_data_correlation()   # Health data correlation across regions
    calculate_pollution_forecasting()     # Pollution forecasting

    # Generate visualizations
    generate_visualizations()


if __name__ == "__main__":
    main()
