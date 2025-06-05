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

# Получение данных из AppMetrica
def fetch_acquisition(date: str):
    params = {
        "id": APP_ID,
        "date1": date,
        "date2": date,
        "group": "Day",
        "metrics": "devices",
        "dimensions": "publisher",
        "accuracy": 1,
        "include_undefined": "true",
        "limit": 10000,
        "currency": "RUB",
        "sort": "-devices",
        "source": "installation",
        "lang": "ru",
        "request_domain": "ru"
    }

    headers = {"Authorization": f"OAuth {TOKEN}"}
    response = requests.get("https://api.appmetrica.yandex.ru/v2/user/acquisition", headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    total = 0
    organic = 0

    for row in data.get("data", []):
        publisher_info = row["dimensions"][0]
        publisher_name = publisher_info.get("name", "").strip().lower()
        devices = row["metrics"][0]
        total += devices
        if publisher_name in ("органика", "organic", "(organic)", "(not set)", "none", ""):
            organic += devices

    return total, organic


def fetch_metrica_users(date: str, counter_id: str, token: str):
    url = "https://api-metrika.yandex.net/stat/v1/data"
    headers = {"Authorization": f"OAuth {token}"}
    params = {
        "ids": counter_id,
        "metrics": "ym:s:users",
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
    total = 0
    organic = 0

    for row in data.get("data", []):
        source_id = row["dimensions"][0]["id"].lower()
        users = row["metrics"][0]
        total += users
        if source_id in organic_ids:
            organic += users

    non_organic = total - organic
    return round(organic), round(non_organic)


def fetch_metrica_new_users(date: str, counter_id: str, token: str):
    url = "https://api-metrika.yandex.net/stat/v1/data"
    headers = {"Authorization": f"OAuth {token}"}
    params = {
        "ids": counter_id,
        "metrics": "ym:s:newUsers",
        "date1": date,
        "date2": date,
        "accuracy": "full"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    new_users = data.get("totals", [0])[0]
    return round(new_users)


def fetch_goal_conversion(date: str, counter_id: str, token: str, goal_id: str):
    url = "https://api-metrika.yandex.net/stat/v1/data"
    headers = {"Authorization": f"OAuth {token}"}
    params = {
        "ids": counter_id,
        "metrics": f"ym:s:goal{goal_id}conversionRate",
        "date1": date,
        "date2": date,
        "accuracy": "full"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    conversion = data.get("totals", [0])[0]
    return round(conversion, 2)



# Обновление таблицы
def update_google_sheet(date_str: str, organic: int, non_organic: int, ym_organic: int, ym_non_organic: int,
                        new_users: int, vod_conversion: float, simple_conversion: float):
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=[
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_KEY).worksheet(GSHEET_TAB_NAME)

    header_row = sheet.row_values(1)
    try:
        col_index = header_row.index(date_str) + 1
    except ValueError:
        raise Exception(f"Дата {date_str} не найдена в заголовке таблицы")

    sheet.update_cell(2, col_index, organic)
    sheet.update_cell(3, col_index, non_organic)

    def col_num_to_letter(n):
        result = ""
        while n:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    col_letter = col_num_to_letter(col_index)

    formatted_vod = str(round(vod_conversion, 2)).replace(".", ",")
    formatted_simple = str(round(simple_conversion, 2)).replace(".", ",")

    # Формулы
    sheet.update_acell(f"{col_letter}4", f"={col_letter}11/{col_letter}3")
    sheet.update_acell(f"{col_letter}5", f"=ЕСЛИ({col_letter}7=0;{col_letter}11;{col_letter}11/{col_letter}7)")

    # Метрика
    sheet.update_cell(15, col_index, ym_organic)
    sheet.update_cell(16, col_index, ym_non_organic)
    sheet.update_cell(18, col_index, new_users)   
    sheet.update_cell(19, col_index, formatted_vod)     
    sheet.update_cell(20, col_index, formatted_simple)

    print(f"Обновлены строки 2–5, 15–16, 18-20 для {col_letter}")


date_obj = datetime.now() - timedelta(days=1)
date_api = date_obj.strftime("%Y-%m-%d")
date_gs = date_obj.strftime("%d.%m.%y")

total, organic = fetch_acquisition(date_api)
non_organic = total - organic

ym_organic, ym_non_organic = fetch_metrica_users(date_api, COUNTER_ID, TOKEN)
new_users = fetch_metrica_new_users(date_api, COUNTER_ID, TOKEN)

vod_conversion = fetch_goal_conversion(date_api, COUNTER_ID, TOKEN, GOAL_ID_VOD)
simple_conversion = fetch_goal_conversion(date_api, COUNTER_ID, TOKEN, GOAL_ID_SIMPLE)

update_google_sheet(
    date_gs,
    organic,
    non_organic,
    ym_organic,
    ym_non_organic,
    new_users,
    vod_conversion,
    simple_conversion
)