import requests
import gspread
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import pymysql

# === Настройки ===
app_id = 4661140
token = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
counter_id = "99133990"
id_vod = "353102040"
id_simple = "353101995"
gs_key = "1tE8lfckXvX536H2D64L4coqYRbZpl8eMhQQ2N6gAchw"
gs_table_name = "За день"
credentials = "limehd-451312-44fe1c22414e.json"

# === Функции ===

def fetch_appmetrica(date: str):
    url = "https://api.appmetrica.yandex.ru/v2/user/acquisition"
    headers = {"Authorization": f"OAuth {token}"}
    params = {
        "id": app_id,
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
    headers = {"Authorization": f"OAuth {token}"}
    metrics = ",".join([
        "ym:s:users",
        "ym:s:newUsers",
        f"ym:s:goal{id_vod}conversionRate",
        f"ym:s:goal{id_simple}conversionRate"
    ])
    params = {
        "ids": counter_id,
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

    totals = data.get("totals", [0, 0, 0, 0])
    new_users = round(totals[1])
    vod_conversion = round(totals[2], 2)
    simple_conversion = round(totals[3], 2)

    return ym_organic, ym_non_organic, new_users, vod_conversion, simple_conversion


def update_google_sheet(date_str: str, app_organic, app_non_organic,
                        ym_organic, ym_non_organic, new_users,
                        vod_conv, simple_conv):
    creds = Credentials.from_service_account_file(credentials, scopes=[
        "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(gs_key).worksheet(gs_table_name)
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

    return {
        "Дата": date_str,
        "Установки органика": app_organic,
        "Установки неорганика": app_non_organic,
        "Посетители органика": ym_organic,
        "Посетители неорганика": ym_non_organic,
        "Новые посетители": new_users,
        "Старт просмотра VOD": vod_conv,
        "Старт просмотра": simple_conv
    }


def insert_into_day(row):
    conn = pymysql.connect(
        host='172.19.95.127',
        user='DataAnalyst',
        password='64fjeObn./d,DP][xds',
        database='pix',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        sql = """
        INSERT INTO day (
            date, app_organic, app_non_organic,
            ym_organic, ym_non_organic, new_users,
            vod_conversion, simple_conversion
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            app_organic = VALUES(app_organic),
            app_non_organic = VALUES(app_non_organic),
            ym_organic = VALUES(ym_organic),
            ym_non_organic = VALUES(ym_non_organic),
            new_users = VALUES(new_users),
            vod_conversion = VALUES(vod_conversion),
            simple_conversion = VALUES(simple_conversion)
        """
        cursor.execute(sql, (
            row["Дата"],
            row["Установки органика"],
            row["Установки неорганика"],
            row["Посетители органика"],
            row["Посетители неорганика"],
            row["Новые посетители"],
            row["Старт просмотра VOD"],
            row["Старт просмотра"]
        ))

    conn.commit()
    conn.close()
    print("Данные добавлены в таблицу day")


#Основной блок
date_obj = datetime.now() - timedelta(days=1)
date_api = date_obj.strftime("%Y-%m-%d")
date_gs = date_obj.strftime("%d.%m.%y")

app_org, app_non_org = fetch_appmetrica(date_api)
ym_org, ym_non_org, new_users, vod_conv, simple_conv = fetch_metrika_combined(date_api)

result_row = update_google_sheet(
    date_gs, app_org, app_non_org,
    ym_org, ym_non_org, new_users,
    vod_conv, simple_conv
)

df_result = pd.DataFrame([result_row])
print("\nДанные, выгруженные в таблицу:")
print(df_result)

insert_into_day(result_row)
