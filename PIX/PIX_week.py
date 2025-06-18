import requests
import pandas as pd
from datetime import datetime, timedelta
import pymysql
from collections import defaultdict

# Авторизация и параметры
token = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
headers = {"Authorization": f"OAuth {token}"}
main_app_id = 4661140
app_ids = [369184, 2294012, 2735686, 3158110, 4183801]

# Расчёт прошлой недели (Пн–Вс)
today = datetime.today()
last_monday = today - timedelta(days=today.weekday() + 7)
last_sunday = last_monday + timedelta(days=6)

date1 = last_monday.strftime('%Y-%m-%d')
date2 = last_sunday.strftime('%Y-%m-%d')

parsed_rows = []
pix_clicks = {}
pix_installs = {}

# Сбор user/acquisition по основному приложению
params = {
    "id": main_app_id,
    "date1": date1,
    "date2": date2,
    "group": "Day",
    "metrics": "impressions,clicks,devices",
    "dimensions": "date,campaign",
    "limit": 10000,
    "accuracy": "1",
    "include_undefined": "true",
    "currency": "RUB",
    "sort": "-devices",
    "source": "installation",
    "lang": "ru",
    "request_domain": "ru"
}
response = requests.get("https://api.appmetrica.yandex.ru/v2/user/acquisition", headers=headers, params=params)

if response.status_code == 200:
    for row in response.json().get("data", []):
        dimensions = [d["name"] for d in row["dimensions"]]
        metrics = row["metrics"]
        parsed = {
            "date": dimensions[0],
            "campaign": dimensions[1],
            "impressions": int(metrics[0]),
            "clicks": int(metrics[1]),
            "installations": int(metrics[2])
        }
        parsed_rows.append(parsed)
        
        # если это PIX Android push, сохраняем клики/установки по датам
        if parsed["campaign"] == "PIX Android push":
            pix_clicks[parsed["date"]] = parsed["clicks"]
            pix_installs[parsed["date"]] = parsed["installations"]
else:
    print(f"Ошибка {response.status_code} при user/acquisition: {response.text}")

# Сбор показов из других приложений с фильтром по pix
aggregated_pix = defaultdict(int)

for app_id in app_ids:
    url = "https://api.appmetrica.yandex.ru/stat/v1/data"
    params = {
        "ids": app_id,
        "date1": date1,
        "date2": date2,
        "group": "Day",
        "metrics": "ym:pc:receivedEvents",
        "dimensions": "ym:pc:date,ym:pc:campaignInfo",
        "limit": 10000,
        "accuracy": "medium",
        "include_undefined": "true",
        "currency": "RUB",
        "sort": "-ym:pc:receivedEvents",
        "lang": "ru",
        "request_domain": "ru"
    }
    r = requests.get(url, headers=headers, params=params)
    if r.status_code == 200:
        for item in r.json().get("data", []):
            campaign_name = item["dimensions"][1]["name"]
            if "pix" in campaign_name.lower():
                actual_date = item["dimensions"][0]["name"]
                impressions = int(item["metrics"][0])
                aggregated_pix[actual_date] += impressions
    else:
        print(f"Ошибка {r.status_code} при получении pix-данных: {r.text}")

# Добавляем агрегированные данные по PIX Android push
for date_key, impressions_sum in aggregated_pix.items():
    parsed_rows.append({
        "date": date_key,
        "campaign": "PIX Android push",
        "impressions": impressions_sum,
        "clicks": pix_clicks.get(date_key, 0),
        "installations": pix_installs.get(date_key, 0)
    })

# Сохраняем в DataFrame и вставляем в БД
df = pd.DataFrame(parsed_rows)

def insert_row_to_sql(row):
    conn = pymysql.connect(
        host='172.19.95.127',
        user='DataAnalyst',
        password='64fjeObn./d,DP][xds',
        database='pix',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    with conn.cursor() as cursor:
        check_sql = """
        SELECT impressions, clicks, installations
        FROM week
        WHERE date = %s AND campaign = %s
        LIMIT 1
        """
        cursor.execute(check_sql, (row["date"], row["campaign"]))
        existing = cursor.fetchone()

        if not existing:
            insert_sql = """
            INSERT INTO week (date, campaign, impressions, clicks, installations)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                row["date"],
                row["campaign"],
                row["impressions"],
                row["clicks"],
                row["installations"]
            ))
            print(f"Добавлено: {row['date']} — {row['campaign']}")
        else:
            if (existing["impressions"] != row["impressions"] or
                existing["clicks"] != row["clicks"] or
                existing["installations"] != row["installations"]):
                update_sql = """
                UPDATE week
                SET impressions = %s,
                    clicks = %s,
                    installations = %s
                WHERE date = %s AND campaign = %s
                """
                cursor.execute(update_sql, (
                    row["impressions"],
                    row["clicks"],
                    row["installations"],
                    row["date"],
                    row["campaign"]
                ))
                print(f"Обновлено: {row['date']} — {row['campaign']}")
    conn.commit()
    conn.close()

for _, row in df.iterrows():
    insert_row_to_sql(row)

print("\nОбработка завершена.")
