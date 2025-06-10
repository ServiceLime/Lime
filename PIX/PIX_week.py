import requests
import pandas as pd
from datetime import datetime, timedelta
import pymysql

# Авторизация и параметры
token = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
app_id = 4661140
url = "https://api.appmetrica.yandex.ru/v2/user/acquisition"

# Расчёт прошлой недели (Пн–Вс)
today = datetime.today()
last_monday = today - timedelta(days=today.weekday() + 7)
last_sunday = last_monday + timedelta(days=6)

date1 = last_monday.strftime('%Y-%m-%d')
date2 = last_sunday.strftime('%Y-%m-%d')
date = datetime.now() - timedelta(days=1)
date = date.strftime('%Y-%m-%d')

# Параметры запроса
params = {
    "id": app_id,
    "date1": date,
    "date2": date,
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

headers = {
    "Authorization": f"OAuth {token}"
}

# Запрос и сохранение в DataFrame
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    json_data = response.json()
    raw_data = json_data.get("data", [])

    if raw_data:
        parsed_rows = []
        for row in raw_data:
            dimensions = [d["name"] for d in row["dimensions"]]
            metrics = row["metrics"]

            parsed_row = {
                "date": dimensions[0],
                "campaign": dimensions[1],
                "impressions": int(metrics[0]),
                "clicks": int(metrics[1]),
                "installations": int(metrics[2])
            }

            parsed_rows.append(parsed_row)

        df = pd.DataFrame(parsed_rows)
        print(f"Данные за {date}:")
        print(df)

        # Вставка в SQL с проверкой и сравнением значений
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
                        existing["installations"] != row["installations"]
                    ):
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

        print("\nЗагрузка завершена.")
    else:
        print("Нет данных за указанный период.")
else:
    print(f"Ошибка {response.status_code}: {response.text}")
