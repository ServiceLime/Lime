import csv
import os
import sys
from datetime import datetime, timedelta
from typing import Tuple

import requests

APP_ID = 4661140
API_URL = "https://api.appmetrica.yandex.ru/logs/v1/export/installations.csv"
TOKEN_ENV = "APPMETRICA_TOKEN"


def fetch_installations(date: str) -> Tuple[int, int]:
    token = os.getenv(TOKEN_ENV)
    if not token:
        raise RuntimeError(f"Environment variable {TOKEN_ENV} is required")

    params = {
        "application_id": APP_ID,
        "date_since": date,
        "date_until": date,
        "fields": "tracker_name"
    }

    headers = {"Authorization": f"OAuth {token}"}
    response = requests.get(API_URL, params=params, headers=headers)
    response.raise_for_status()

    total = 0
    organic = 0
    reader = csv.DictReader(response.text.splitlines(), delimiter=';')
    for row in reader:
        total += 1
        tracker = row.get("tracker_name", "").strip().lower()
        if tracker in ("", "organic", "(organic)"):
            organic += 1
    return total, organic


def main():
    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    total, organic = fetch_installations(date)
    print(f"Date: {date}")
    print(f"Organic installs: {organic}")
    print(f"Total installs: {total} (non-organic: {total - organic})")


if __name__ == "__main__":
    main()
