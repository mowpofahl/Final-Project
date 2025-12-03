import requests
from bs4 import BeautifulSoup

from db.db_operations import store_health_data

URL = "https://www.cdc.gov/nchs/data/nhis/earlyrelease/ER_aia.htm"  # Example placeholder URL


def fetch_health_data():
    """
    Fetch health data using BeautifulSoup from a CDC webpage.
    """
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Failed to fetch health data: {exc}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract data (adjust according to actual page structure)
    data = soup.find_all('tr')  # Assuming data is in table rows
    for row in data:
        cells = row.find_all('td')
        if cells:
            state = cells[0].text
            asthma_rate = cells[1].text
            copd_rate = cells[2].text
            year = cells[3].text

            # Store data in the database
            store_health_data(state, asthma_rate, copd_rate, year)
