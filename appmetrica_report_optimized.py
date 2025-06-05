
import requests
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# Настройки
APP_ID = 4661140
TOKEN = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
COUNTER_ID = "99133990"
GOAL_ID_VOD = "353102040"
GOAL_ID_SIMPLE = "353101995"
SPREADSHEET_KEY = "1tE8lfckXvX536H2D64L4coqYRbZpl8eMhQQ2N6gAchw"
GSHEET_TAB_NAME = "За день"
CREDENTIALS_FILE = "limehd-451312-44fe1c22414e.json"

def fetch_appmetrica(date: str):
    url = "https://api.appmetrica.yandex.ru/v2/user/acquisition"
    headers = {"Authorization": f"OAuth {TOKEN}"}
    params = {
        "id": APP_ID,
        "date1": date,
        "date2": date,
        "group": "Day",
        "metrics": "devices",
        "dimensions": "publisher",
        "include_undefined": "true",
        "limit": 10000,
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    total = 0
    organic = 0

    for row in data.get("data", []):
        publisher = row["dimensions"][0].get("name", "").strip().lower()
        devices = row["metrics"][0]
        total += devices
        if publisher in {"органика", "organic", "(organic)", "(not set)", "none", ""}:
            organic += devices

    return round(organic), round(total - organic)

def fetch_metrika_combined(date: str):
    url = "https://api-metrika.yandex.net/stat/v1/data"
    headers = {"Authorization": f"OAuth {TOKEN}"}
    metrics = ",".join([
        "ym:s:users",
        "ym:s:newUsers",
        f"ym:s:goal{GOAL_ID_VOD}conversionRate",
        f"ym:s:goal{GOAL_ID_SIMPLE}conversionRate"
    ])
    params = {
        "ids": COUNTER_ID,
        "metrics": metrics,
        "dimensions": "ym:s:lastTrafficSource",
        "date1": date,
        "date2": date,
        "accuracy": "full",
        "limit": 100
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    organic_ids = {"direct", "organic", "internal"}
    total_users = 0
    organic_users = 0
    for row in data.get("data", []):
        src = row["dimensions"][0]["id"].lower()
        users = row["metrics"][0]
        total_users += users
        if src in organic_ids:
            organic_users += users

    ym_non_organic = round(total_users - organic_users)
    ym_organic = round(organic_users)

    # Тоталы: [users, newUsers, goal_vod, goal_simple]
    totals = data.get("totals", [0, 0, 0, 0])
    new_users = round(totals[1])
    vod_conversion = round(totals[2], 2)
    simple_conversion = round(totals[3], 2)

    return ym_organic, ym_non_organic, new_users, vod_conversion, simple_conversion

def update_google_sheet(date_str: str, app_organic, app_non_organic,
                        ym_organic, ym_non_organic, new_users,
                        vod_conv, simple_conv):
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=[
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(GSHEET_TAB_NAME)
    header_row = sheet.row_values(1)
    try:
        col = header_row.index(date_str) + 1
    except ValueError:
        raise Exception(f"Дата {date_str} не найдена в таблице")

    def col_letter(n):
        result = ""
        while n:
            n, r = divmod(n - 1, 26)
            result = chr(65 + r) + result
        return result

    letter = col_letter(col)

    sheet.update_cell(2, col, app_organic)
    sheet.update_cell(3, col, app_non_organic)
    sheet.update_acell(f"{letter}4", f"={letter}11/{letter}3")
    sheet.update_acell(f"{letter}5", f"=ЕСЛИ({letter}7=0;{letter}11;{letter}11/{letter}7)")
    sheet.update_cell(15, col, ym_organic)
    sheet.update_cell(16, col, ym_non_organic)
    sheet.update_cell(18, col, new_users)
    sheet.update_cell(19, col, str(vod_conv).replace(".", ","))
    sheet.update_cell(20, col, str(simple_conv).replace(".", ","))

    print(f"Обновлены строки 2–5, 15–16, 18–20 для {letter}")


date_obj = datetime.now() - timedelta(days=1)
date_api = date_obj.strftime("%Y-%m-%d")
date_gs = date_obj.strftime("%d.%m.%y")

app_org, app_non_org = fetch_appmetrica(date_api)
ym_org, ym_non_org, new_users, vod_conv, simple_conv = fetch_metrika_combined(date_api)

update_google_sheet(date_gs, app_org, app_non_org, ym_org, ym_non_org, new_users, vod_conv, simple_conv)
