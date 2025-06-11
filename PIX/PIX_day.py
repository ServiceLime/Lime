import os
import requests
import gspread
import pandas as pd
import pymysql
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1
from gspread import Cell

# Константы
app_id = 4661140
token = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
counter_id = "99133990"
id_vod = "353102040"
id_simple = "353101995"
gs_key = "1tE8lfckXvX536H2D64L4coqYRbZpl8eMhQQ2N6gAchw"
gs_table_name = "За день"
credentials_file = "limehd-451312-44fe1c22414e.json"

organic_sources = {"органика", "organic", "(organic)", "(not set)", "none", ""}
yandex_organic_ids = {"direct", "organic", "internal"}

@dataclass
class DailyMetrics:
    date: str
    app_organic: int
    app_non_organic: int
    ym_organic: int
    ym_non_organic: int
    new_users: int
    vod_conversion: float
    simple_conversion: float

def col_letter(n: int) -> str:
    result = ""
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result

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

    total = organic = 0
    for row in data.get("data") or []:
        publisher = row["dimensions"][0].get("name", "").strip().lower()
        devices = row["metrics"][0]
        total += devices
        if publisher in organic_sources:
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

    total_users = organic_users = 0
    for row in data.get("data") or []:
        src = row["dimensions"][0]["id"].lower()
        users = row["metrics"][0]
        total_users += users
        if src in yandex_organic_ids:
            organic_users += users

    totals = data.get("totals", [0, 0, 0, 0])
    return (
        round(organic_users),
        round(total_users - organic_users),
        round(totals[1]),
        round(totals[2], 2),
        round(totals[3], 2)
    )

def update_google_sheet(metrics: DailyMetrics):
    creds = Credentials.from_service_account_file(credentials_file, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)
    sheet = client.open_by_key(gs_key).worksheet(gs_table_name)

    header_row = sheet.row_values(1)
    current_col_count = sheet.col_count

    if metrics.date not in header_row:
        col = len(header_row) + 1
        if col > current_col_count:
            sheet.add_cols(col - current_col_count)
        sheet.update_cell(1, col, metrics.date)
        print(f"Добавлен новый столбец: {metrics.date}")
    else:
        col = header_row.index(metrics.date) + 1

    letter = col_letter(col)

    cells = [
        Cell(2, col, metrics.app_organic),
        Cell(3, col, metrics.app_non_organic),
        Cell(15, col, metrics.ym_organic),
        Cell(16, col, metrics.ym_non_organic),
        Cell(18, col, metrics.new_users),
        Cell(19, col, str(metrics.vod_conversion).replace(".", ",")),
        Cell(20, col, str(metrics.simple_conversion).replace(".", ","))
    ]
    sheet.update_cells(cells, value_input_option="USER_ENTERED")

    sheet.update_acell(f"{letter}4", f"={letter}11/{letter}3")
    sheet.update_acell(f"{letter}5", f"=ЕСЛИ({letter}7=0;{letter}11;{letter}11/{letter}7)")
    print(f"Обновлены строки 2–5, 15–20 для {letter}")

def insert_into_day(metrics: DailyMetrics):
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
            metrics.date,
            metrics.app_organic,
            metrics.app_non_organic,
            metrics.ym_organic,
            metrics.ym_non_organic,
            metrics.new_users,
            metrics.vod_conversion,
            metrics.simple_conversion
        ))
    conn.commit()
    conn.close()
    print("Данные добавлены в таблицу day")

if __name__ == "__main__":
    date_obj = datetime.now() - timedelta(days=1)
    date_api = date_obj.strftime("%Y-%m-%d")
    date_gs = date_obj.strftime("%d.%m.%y")

    app_org, app_non_org = fetch_appmetrica(date_api)
    ym_org, ym_non_org, new_users, vod_conv, simple_conv = fetch_metrika_combined(date_api)

    metrics = DailyMetrics(
        date=date_gs,
        app_organic=app_org,
        app_non_organic=app_non_org,
        ym_organic=ym_org,
        ym_non_organic=ym_non_org,
        new_users=new_users,
        vod_conversion=vod_conv,
        simple_conversion=simple_conv
    )

    update_google_sheet(metrics)
    insert_into_day(metrics)

    df_result = pd.DataFrame([metrics.__dict__])
    print("\nДанные, выгруженные в таблицу:")
    print(df_result)
