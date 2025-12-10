import io
import random
import re
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from db.db_operations import get_ingestion_index, store_health_data, update_ingestion_index

URL = "https://coepht.colorado.gov/asthma-data"
STATE_NAME = "Colorado"
MAX_ROWS_PER_RUN = 25
_SHUFFLE_SEED = 2024


def _parse_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:
            return None
        return float(value)
    value = value.strip()
    if not value:
        return None
    return float(value)


def _parse_int(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:
            return None
        return int(float(value))
    value = value.strip()
    if not value:
        return None
    return int(float(value))


def _extract_drive_file_id(href):
    if not href:
        return None
    match = re.search(r"/d/([^/]+)/", href)
    if match:
        return match.group(1)
    parsed = urlparse(href)
    query = parse_qs(parsed.query)
    ids = query.get("id")
    if ids:
        return ids[0]
    return None


def _normalize_fips(value):
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text or None


def fetch_health_data():
    """
    Download Colorado asthma data and insert ≤25 new rows per run.
    """
    # Grab the landing page first so we can follow the Google Drive link they hide in there.
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Failed to fetch health data: {exc}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    download_link = None
    for anchor in soup.find_all('a'):
        if "download asthma data" in anchor.get_text(strip=True).lower():
            download_link = anchor
            break
    if not download_link or not download_link.get('href'):
        print("Could not find the asthma download link on the source page.")
        return

    download_href = urljoin(URL, download_link['href'])
    file_id = _extract_drive_file_id(download_href)
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}" if file_id else download_href

    # Pull down the giant Excel file and turn it into a DataFrame we can filter.
    try:
        file_response = requests.get(download_url, timeout=60)
        file_response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Failed to download asthma workbook: {exc}")
        return

    try:
        workbook = pd.read_excel(io.BytesIO(file_response.content))
    except Exception as exc:
        print(f"Failed to parse asthma workbook: {exc}")
        return

    required_cols = {'COUNTY', 'RATE', 'L95CL', 'U95CL', 'VISITS', 'YEAR', 'GENDER', 'MEASURE', 'HEALTHOUTCOMEID', 'cofips'}
    missing = required_cols.difference(workbook.columns)
    if missing:
        print(f"Asthma dataset is missing required columns: {', '.join(sorted(missing))}")
        return

    rows = workbook.to_dict(orient='records')

    records = []
    for row in rows:
        outcome = (str(row.get('HEALTHOUTCOMEID') or '')).strip().lower()
        measure = (str(row.get('MEASURE') or '')).strip().lower()
        if outcome != "asthma" or measure != "age adjusted rate":
            continue
        rate = _parse_float(row.get('RATE'))
        year = _parse_int(row.get('YEAR'))
        if rate is None or year is None:
            continue
        lower_ci = _parse_float(row.get('L95CL'))
        upper_ci = _parse_float(row.get('U95CL'))
        visits = _parse_int(row.get('VISITS')) or 0
        gender = row.get('GENDER') or "Both genders"
        county = row.get('COUNTY') or "Statewide"
        fips = _normalize_fips(row.get('cofips'))
        records.append(
            {
                'county': county,
                'rate': rate,
                'lower_ci': lower_ci,
                'upper_ci': upper_ci,
                'visits': visits,
                'gender': gender,
                'year': year,
                'fips': fips,
            }
        )

    if not records:
        print("No asthma rows detected in the downloaded workbook.")
        return

    rng = random.Random(_SHUFFLE_SEED)
    rng.shuffle(records)

    last_index = get_ingestion_index('health')
    next_index = last_index + 1
    if next_index >= len(records):
        print("All available health records have already been ingested.")
        return

    inserted = 0
    idx = next_index
    while idx < len(records) and inserted < MAX_ROWS_PER_RUN:
        record = records[idx]
        success = store_health_data(
            county_name=record['county'],
            state_name=STATE_NAME,
            fips=record['fips'],
            asthma_rate=record['rate'],
            lower_ci=record['lower_ci'],
            upper_ci=record['upper_ci'],
            visits=record['visits'],
            gender=record['gender'],
            year=record['year'],
        )
        if success:
            inserted += 1
        idx += 1

    update_ingestion_index('health', min(idx - 1, len(records) - 1))
    if inserted == 0:
        print("No new health records were inserted; database may already be up to date.")
    else:
        print(f"Inserted {inserted} new health records (through index {idx - 1}).")
