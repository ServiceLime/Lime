#!/usr/bin/env python
# coding: utf-8

# # SQL

# In[ ]:


from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import os

# Параметры подключения
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME", "payments_yakassa")

if not host or not user or not password:
    raise RuntimeError("Database credentials must be provided via environment variables")

# Создание движка
engine = create_engine(
    f"mysql+mysqldb://{user}:%s@{host}/{database}" % quote_plus(password)
)

# SQL-запрос
query = """
SELECT id, order_id, price, status, created_date, pack_id, pack_identifier_id, user_id, email, device, device_name,
       cancel_reason, is_refund, refund_amount, user_pack_id, platform_id, app_id, is_autopay, is_promo
FROM payments_yakassa
WHERE app_id IN (561, 582)
"""

# Загрузка данных в DataFrame
with engine.connect() as connection:
    df = pd.read_sql(text(query), connection)

print(f"Загружено {len(df)} строк")


# # Недельный

# In[11]:


import pandas as pd
from datetime import datetime, timedelta

FILE_PATH = 'export_(ReportPayments.2025-02-14 - 2025-06-01.).csv'  # путь к CSV-файлу

# === Загрузка данных ===
with open(FILE_PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

header_line = lines[10].strip().split(';')
df = pd.read_csv(FILE_PATH, skiprows=11, sep=';', names=header_line)

# === Подготовка ===
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df['дата'] = pd.to_datetime(df['дата'], errors='coerce')

df = df[
    (df['статус'].str.lower() == 'успешно') &
    (df['e-mail_оплаты'].notnull())
].sort_values(by=['e-mail_оплаты', 'id_пакета', 'дата'])

# === Классификация переходов ===
def classify_transactions(group):
    group = group.sort_values('дата')
    types = []
    promo_found = False
    full_found = False
    for _, row in group.iterrows():
        is_auto = str(row['тип_платежа']).strip().lower() == 'автоплатеж'
        is_promo = row['промо'] == 1
        if is_promo:
            types.append('promo')
            promo_found = True
        elif promo_found and not full_found and is_auto:
            types.append('full')
            full_found = True
        elif full_found and is_auto:
            types.append('repeat')
        else:
            types.append('other')
    group['тип_сделки'] = types
    return group

df = df.groupby(['e-mail_оплаты', 'id_пакета']).apply(classify_transactions).reset_index(drop=True)

# === Только первая автопокупка после промо ===
df = df[df['тип_сделки'] == 'full']
df['год'] = df['дата'].dt.isocalendar().year
df['неделя'] = df['дата'].dt.isocalendar().week

# Пользователь с привязкой к пакету
df['пользователь'] = df['e-mail_оплаты'] + ' — ' + df['девайс_оплаты']

# === Группировка по неделе и пакету ===
grouped = (
    df.groupby(['год', 'неделя', 'пакет'])
    .agg(переходы=('пользователь', lambda x: sorted(set(x))))
    .reset_index()
)

# Добавляем границы недели
def get_week_date_range(year, week):
    start = datetime.fromisocalendar(year, week, 1)
    end = start + timedelta(days=6)
    return start.strftime('%d.%m'), end.strftime('%d.%m')

grouped[['начало_недели', 'конец_недели']] = grouped.apply(
    lambda row: pd.Series(get_week_date_range(row['год'], row['неделя'])),
    axis=1
)

# === Сортировка и вывод ===
grouped = grouped.sort_values(by=['год', 'неделя'], ascending=False)

# === Печать отчёта ===
print("Переходы по неделям и пакетам:\n")
total = 0
for (_, row) in grouped.iterrows():
    count = len(row['переходы'])
    total += count
    print(f"{row['неделя']} Неделя ({row['начало_недели']}–{row['конец_недели']}) — {row['пакет']}: {count}")

print(f"\nВсего переходов: {total}\n")

# Детальный список
for _, row in grouped.iterrows():
    print(f"{row['неделя']} Неделя ({row['начало_недели']}–{row['конец_недели']}) — {row['пакет']}:")
    for u in row['переходы']:
        print(f"  {u}")
    print()


# # Месячный

# In[12]:


import pandas as pd
from datetime import datetime

FILE_PATH = 'export_(ReportPayments.2025-02-14 - 2025-06-01.).csv'

# === Загрузка данных ===
with open(FILE_PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

header_line = lines[10].strip().split(';')
df = pd.read_csv(FILE_PATH, skiprows=11, sep=';', names=header_line)

# === Подготовка ===
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df['дата'] = pd.to_datetime(df['дата'], errors='coerce')

df = df[
    (df['статус'].str.lower() == 'успешно') &
    (df['e-mail_оплаты'].notnull())
].sort_values(by=['e-mail_оплаты', 'id_пакета', 'дата'])

# === Классификация переходов ===
def classify_transactions(group):
    group = group.sort_values('дата')
    types = []
    promo_found = False
    full_found = False
    for _, row in group.iterrows():
        is_auto = str(row['тип_платежа']).strip().lower() == 'автоплатеж'
        is_promo = row['промо'] == 1
        if is_promo:
            types.append('promo')
            promo_found = True
        elif promo_found and not full_found and is_auto:
            types.append('full')
            full_found = True
        elif full_found and is_auto:
            types.append('repeat')
        else:
            types.append('other')
    group['тип_сделки'] = types
    return group

df = df.groupby(['e-mail_оплаты', 'id_пакета']).apply(classify_transactions).reset_index(drop=True)

# === Только первая автопокупка после промо ===
df = df[df['тип_сделки'] == 'full']
df['год'] = df['дата'].dt.year
df['месяц'] = df['дата'].dt.month
df['месяц_текст'] = df['дата'].dt.strftime('%B')
df['пользователь'] = df['e-mail_оплаты'] + ' — ' + df['девайс_оплаты']

# === Группировка по месяцу и пакету ===
grouped = (
    df.groupby(['год', 'месяц', 'месяц_текст', 'пакет'])
    .agg(
        переходы=('пользователь', lambda x: sorted(set(x)))
    )
    .reset_index()
)

# === Сортировка и вывод ===
grouped = grouped.sort_values(by=['год', 'месяц'], ascending=False)

print("Переходы по месяцам и пакетам:\n")
total = 0
for _, row in grouped.iterrows():
    count = len(row['переходы'])
    total += count
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}: {count}")

print(f"\nВсего переходов: {total}\n")

# === Детальный список ===
for _, row in grouped.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}:")
    for u in row['переходы']:
        print(f"  {u}")
    print()


# # Табличка с соответствиями

# In[26]:


import pandas as pd
from IPython.display import display

# Пути к файлам
installations_path = "installations.csv"
payments_path = "ReportPayments.2025-05-01 - 2025-05-31.2025-06-03.csv"

# Загрузка CSV
installations_df = pd.read_csv(installations_path)
payments_df = pd.read_csv(payments_path, sep=';')

# Очистка от пустых profile_id
installations_df_clean = installations_df.dropna(subset=['profile_id'])

# Переименование для объединения
installations_df_clean = installations_df_clean.rename(columns={"profile_id": "Девайс оплаты"})

# Объединение по 'Девайс оплаты'
merged_df = payments_df.merge(installations_df_clean, on="Девайс оплаты", how="inner")

# Сбор нужных столбцов и переименование
result_df = merged_df[["Сумма", "Пакет", "Девайс оплаты", "tracker_name"]]
result_df = result_df.rename(columns={"tracker_name": "Трекер"})

# Вывод таблицы
print("Сводная таблица оплат:")
display(result_df)

# Сумма по трекерам
sum_by_tracker = result_df.groupby("Трекер")["Сумма"].sum().reset_index()
sum_by_tracker = sum_by_tracker.sort_values(by="Сумма", ascending=False)

# Сумма и количество платежей по трекерам
tracker_summary = (
    result_df.groupby("Трекер")
    .agg(
        Сумма=('Сумма', 'sum'),
        Платежи=('Сумма', 'count')
    )
    .reset_index()
    .sort_values(by="Сумма", ascending=False)
)

print("\nСумма и количество платежей по каждому трекеру:")
display(tracker_summary)


# # Вывод конверсии

# In[32]:


import pandas as pd
from datetime import datetime

# === Пути к файлам ===
PAYMENTS_PATH = 'export_(ReportPayments.2025-02-14 - 2025-06-01.).csv'
INSTALLATIONS_PATH = 'installations.csv'

# === Загрузка платежей ===
with open(PAYMENTS_PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

header_line = lines[10].strip().split(';')
df = pd.read_csv(PAYMENTS_PATH, skiprows=11, sep=';', names=header_line)
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df['дата'] = pd.to_datetime(df['дата'], errors='coerce')

# === Фильтрация успешных платежей ===
df = df[
    (df['статус'].str.lower() == 'успешно') &
    (df['e-mail_оплаты'].notnull())
].sort_values(by=['e-mail_оплаты', 'id_пакета', 'дата'])

# === Классификация переходов ===
def classify_transactions(group):
    group = group.sort_values('дата')
    types = []
    promo_found = False
    full_found = False
    for _, row in group.iterrows():
        is_auto = str(row['тип_платежа']).strip().lower() == 'автоплатеж'
        is_promo = row['промо'] == 1
        if is_promo:
            types.append('promo')
            promo_found = True
        elif promo_found and not full_found and is_auto:
            types.append('full')
            full_found = True
        elif full_found and is_auto:
            types.append('repeat')
        else:
            types.append('other')
    group['тип_сделки'] = types
    return group

df = df.groupby(['e-mail_оплаты', 'id_пакета']).apply(classify_transactions).reset_index(drop=True)

# === Фильтрация только переходов full ===
df = df[df['тип_сделки'] == 'full']

# === Загрузка трекеров ===
install_df = pd.read_csv(INSTALLATIONS_PATH)
install_df = install_df.dropna(subset=['profile_id'])
install_df = install_df.rename(columns={'profile_id': 'девайс_оплаты'})
install_df = install_df.drop_duplicates(subset=['девайс_оплаты'])
install_df['tracker_name'] = install_df['tracker_name'].fillna('unknown')

# === Объединение с трекерами ===
df = df.merge(install_df, on='девайс_оплаты', how='left')
df['tracker_name'] = df['tracker_name'].fillna('unknown')

# === Добавление года, месяца и форматированного месяца ===
df['год'] = df['дата'].dt.year
df['месяц'] = df['дата'].dt.month
df['месяц_текст'] = df['дата'].dt.strftime('%B')

# === Создание строки с пользователем и трекером ===
df['пользователь'] = df.apply(
    lambda row: f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽", axis=1
)


# === Группировка по месяцам и пакетам ===
grouped = (
    df.groupby(['год', 'месяц', 'месяц_текст', 'пакет'])
    .agg(переходы=('пользователь', lambda x: sorted(set(x))))
    .reset_index()
    .sort_values(by=['год', 'месяц'], ascending=False)
)

# === Вывод переходов по месяцам и пакетам ===
print("Переходы по месяцам и пакетам:\n")
total = 0
for _, row in grouped.iterrows():
    count = len(row['переходы'])
    total += count
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}: {count}")
print(f"\nВсего переходов: {total}\n")

# === Подробный список переходов с трекерами ===
for _, row in grouped.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}:")
    for u in row['переходы']:
        print(f"  {u}")
    print()

# === Статистика по трекерам с разбивкой по месяцам ===
monthly_tracker_stats = (
    df.groupby(['год', 'месяц', 'месяц_текст', 'tracker_name'])
    .agg(
        сумма=('сумма', 'sum'),
        количество=('e-mail_оплаты', 'nunique')
    )
    .reset_index()
    .sort_values(by=['год', 'месяц', 'сумма'], ascending=[False, False, False])
)

print("\nСтатистика по трекерам по месяцам:\n")
for _, row in monthly_tracker_stats.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['tracker_name']}: {row['количество']} пользователей, {row['сумма']}₽")


# # Вывод повторных

# In[34]:


import pandas as pd
import re

# === Загрузка файлов ===
df = pd.read_csv("ReportPayments.csv", sep=';')
installs_df = pd.read_csv("installations.csv")

# === Полный список исключённых пользователей ===
excluded_block = """
user1997710@limexltd.com — 1ff2e5f148e2d1a0
user1999964@limexltd.com — cd2fa561deea22bb
user2042559@limexltd.com — 7fa1ec440f50276d
user2066121@limexltd.com — 257a5e6cfc28d8e6
user2081500@limexltd.com — 5cc07a327afbd7ad
user2081633@limexltd.com — f3abf8e1419e59c9
user2084776@limexltd.com — 66d2267b52485ce8
user2086911@limexltd.com — 0c9de34d5c8f95e5
user2100827@limexltd.com — edcdff2cf1a6fd36
user2111390@limexltd.com — 8816f5275f15d7e2
user2146950@limexltd.com — 658c6ed9571c7499
user2197525@limexltd.com — 95080a2fc453b5df
user1996219@limexltd.com — 571691c7a5d0e773
user2000062@limexltd.com — 557410aeda7777e3
user2006103@limexltd.com — 4d1ef3cd3c48b5c2
user2157807@limexltd.com — 54f7c434e00ccb61
user2192639@limexltd.com — 9c2fea0649319a5a
user1937657@limexltd.com — bb136a24dde15407
user1951580@limexltd.com — 4b7f7ffa82f5f5fa
user1952308@limexltd.com — b5a6b4c8d5dd0f7d
user1968660@limexltd.com — 67d3b9b2b8f01cc0
user1984935@limexltd.com — c59be577438267d3
user1986447@limexltd.com — fea5f563eca2d711
user2003548@limexltd.com — d87a4a3101676af4
user2016134@limexltd.com — e34856cba4206ac7
user2016141@limexltd.com — 702d3d2afe83aefe
user2018080@limexltd.com — 07923a9431273a66
user2028195@limexltd.com — 60693a4c9ec4b0f2
user2034404@limexltd.com — f0eba7765905cbe7
user2035419@limexltd.com — caf18869878734f8
user2036679@limexltd.com — 35d7c654bc561095
user2037729@limexltd.com — 0b144409c94f72d8
user2049223@limexltd.com — 1502a4f678b484ef
user2059569@limexltd.com — fde0e808f8ef7161
user2059912@limexltd.com — 25aefc92608dde88
user2062971@limexltd.com — 8f3885c969f665b0
user2067304@limexltd.com — b8bd71f66253cfb1
user2070643@limexltd.com — 296e497b3ae9b035
user2072729@limexltd.com — 67a7bc801d89d669
user2073289@limexltd.com — 383ed59dac2f3e31
user2073835@limexltd.com — c58e0fce0211e263
user2073891@limexltd.com — 2d6ad559d23845c8
user2074003@limexltd.com — 674325cb3015a032
user2078168@limexltd.com — 123519a43af3cffe
user2087835@limexltd.com — 6079facf3673948d
user2088402@limexltd.com — cfcb32425bde01b5
user2089536@limexltd.com — afdc4724d22d96bd
user2090201@limexltd.com — dbc63abf008e1f37
user2091244@limexltd.com — bcfbbcb6b234006b
user2093302@limexltd.com — 7ebd9639be4fcaed
user2093610@limexltd.com — d7866c268dd623e5
user2095983@limexltd.com — 293043064dc9c536
user2096144@limexltd.com — ba186ca78ce19ad1
user2098181@limexltd.com — 2d01bd20796af6df
user2100540@limexltd.com — 3f9002b1b1787a70
user2103655@limexltd.com — 666a14050f285414
user2105937@limexltd.com — d964e22325561fa9
user2106049@limexltd.com — cc661fb02668b08f
user2106490@limexltd.com — 18dfee75a99a7bca
user2113014@limexltd.com — 651406020d7d220e
user2124718@limexltd.com — 3a8cc76c159baca5
user2125089@limexltd.com — e108024d4d797ce9
user2127658@limexltd.com — ab9eee01e52c163c
user2130647@limexltd.com — 6a757ed6e183e7fe
user2135449@limexltd.com — feaacaed2cfde627
user2138494@limexltd.com — 553d93d1465bc275
user2143457@limexltd.com — 3593bce2c48eee7e
user2149575@limexltd.com — 2d5035ab37e5e1b1
user2153054@limexltd.com — 06fe145bf7c1b5e6
user2155280@limexltd.com — 283f2eaac4776d4e
user2158073@limexltd.com — 57ed8ebee3684ddb
user2159396@limexltd.com — 675b9552500e4007
user2159522@limexltd.com — 205e06b93b843f8e
user2173249@limexltd.com — dda3379b35abb1c9
user2174705@limexltd.com — 56b48819b52ba167
user2174957@limexltd.com — 9c6e8f9b6bf68207
user2175741@limexltd.com — 39de7b044ab3fa34
user2177295@limexltd.com — f55beafab2af475b
user2181488@limexltd.com — 333f02aea6ba3dd1
user2191183@limexltd.com — fb17695c65476334
user2194683@limexltd.com — 1c47bbcc52a46f56
user2195698@limexltd.com — ce7ca237b697f490
user2196090@limexltd.com — dc128e8609a6c554
"""

excluded = set(re.findall(r'(\S+@limexltd\.com)\s+—\s+([0-9a-f]+)', excluded_block))

# === Удаление исключённых пользователей ===
df_filtered = df[~df.apply(lambda row: (row['E-mail оплаты'], row['Девайс оплаты']) in excluded, axis=1)]

# === Извлечение "старого" трекера (если не будет найден) ===
df_filtered['tracker'] = df_filtered['Приложение'].str.extract(r'\|\s*(.*)')

# === Слияние с installations.csv по device_id ===
merged_df = df_filtered.merge(installs_df, left_on='Девайс оплаты', right_on='profile_id', how='left')

# === Приоритетный трекер — из installations.csv ===
merged_df['tracker_final'] = merged_df['tracker_name'].combine_first(merged_df['tracker'])

# === Формат результата ===
merged_df['output'] = merged_df.apply(
    lambda row: f"{row['E-mail оплаты']} — {row['Девайс оплаты']} — [{row['tracker_final']}] — {row['Сумма']}₽",
    axis=1
)

# === Вывод в консоль ===
for line in merged_df['output']:
    print(line)

# === Преобразуем дату в формат datetime и выделим месяц ===
merged_df['Дата'] = pd.to_datetime(merged_df['Дата'], errors='coerce')
merged_df['Месяц'] = merged_df['Дата'].dt.strftime('%B %Y')

# === Группировка по Месяц и tracker_final ===
stats = (
    merged_df.groupby(['Месяц', 'tracker_final'])
    .agg(пользователей=('E-mail оплаты', 'nunique'), сумма_руб=('Сумма', 'sum'))
    .reset_index()
    .sort_values(['Месяц', 'tracker_final'])
)

# === Вывод статистики в консоль ===
print("\nСтатистика по трекерам по месяцам:\n")
for _, row in stats.iterrows():
    print(f"{row['Месяц']} — {row['tracker_final']}: {row['пользователей']} пользователей, {row['сумма_руб']}₽")


# In[36]:


import pandas as pd
from datetime import datetime

# === Пути к файлам ===
PAYMENTS_PATH = 'export_(ReportPayments.2025-02-14 - 2025-06-01.).csv'
INSTALLATIONS_PATH = 'installations.csv'

# === Загрузка платежей ===
with open(PAYMENTS_PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

header_line = lines[10].strip().split(';')
df = pd.read_csv(PAYMENTS_PATH, skiprows=11, sep=';', names=header_line)
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df['дата'] = pd.to_datetime(df['дата'], errors='coerce')

# === Фильтрация успешных платежей ===
df = df[
    (df['статус'].str.lower() == 'успешно') &
    (df['e-mail_оплаты'].notnull())
].sort_values(by=['e-mail_оплаты', 'id_пакета', 'дата'])

# === Классификация переходов ===
def classify_transactions(group):
    group = group.sort_values('дата')
    types = []
    promo_found = False
    full_found = False

    for _, row in group.iterrows():
        is_auto = str(row['тип_платежа']).strip().lower() == 'автоплатеж'
        is_promo = row['промо'] == 1

        if is_promo:
            types.append('promo')
            promo_found = True
        elif promo_found and not full_found and is_auto:
            types.append('full')
            full_found = True
        elif full_found and is_auto:
            types.append('repeat')
        else:
            types.append('other')
    group['тип_сделки'] = types
    return group

df = df.groupby(['e-mail_оплаты', 'id_пакета']).apply(classify_transactions).reset_index(drop=True)

# === Повторные платежи ===
repeat_df = df[df['тип_сделки'] == 'repeat'].copy()

# === Оставляем только full-переходы для переходной статистики ===
df = df[df['тип_сделки'] == 'full']

# === Загрузка трекеров ===
install_df = pd.read_csv(INSTALLATIONS_PATH)
install_df = install_df.dropna(subset=['profile_id'])
install_df = install_df.rename(columns={'profile_id': 'девайс_оплаты'})
install_df = install_df.drop_duplicates(subset=['девайс_оплаты'])
install_df['tracker_name'] = install_df['tracker_name'].fillna('unknown')

# === Объединение с трекерами ===
df = df.merge(install_df, on='девайс_оплаты', how='left')
df['tracker_name'] = df['tracker_name'].fillna('unknown')

repeat_df = repeat_df.merge(install_df, on='девайс_оплаты', how='left')
repeat_df['tracker_name'] = repeat_df['tracker_name'].fillna('unknown')

# === Добавление года, месяца и форматированного месяца ===
for target_df in [df, repeat_df]:
    target_df['год'] = target_df['дата'].dt.year
    target_df['месяц'] = target_df['дата'].dt.month
    target_df['месяц_текст'] = target_df['дата'].dt.strftime('%B')

# === Создание строки с пользователем и трекером ===
df['пользователь'] = df.apply(
    lambda row: f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽", axis=1
)

# === Группировка по месяцам и пакетам (full only) ===
grouped = (
    df.groupby(['год', 'месяц', 'месяц_текст', 'пакет'])
    .agg(переходы=('пользователь', lambda x: sorted(set(x))))
    .reset_index()
    .sort_values(by=['год', 'месяц'], ascending=False)
)

# === Вывод переходов по месяцам и пакетам ===
print("Переходы по месяцам и пакетам:\n")
total = 0
for _, row in grouped.iterrows():
    count = len(row['переходы'])
    total += count
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}: {count}")
print(f"\nВсего переходов: {total}\n")

# === Подробный список переходов с трекерами ===
for _, row in grouped.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['пакет']}:")
    for u in row['переходы']:
        print(f"  {u}")
    print()

# === Статистика по трекерам с разбивкой по месяцам (FULL) ===
print("\nСтатистика по трекерам (переходы full) по месяцам:\n")
monthly_tracker_stats = (
    df.groupby(['год', 'месяц', 'месяц_текст', 'tracker_name'])
    .agg(
        сумма=('сумма', 'sum'),
        количество=('e-mail_оплаты', 'nunique')
    )
    .reset_index()
    .sort_values(by=['год', 'месяц', 'сумма'], ascending=[False, False, False])
)

for _, row in monthly_tracker_stats.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['tracker_name']}: {row['количество']} пользователей, {row['сумма']}₽")

# === Статистика по повторным платежам (repeat) ===
print("\nСтатистика по повторным платежам (repeat) по месяцам и трекерам:\n")
repeat_stats = (
    repeat_df.groupby(['год', 'месяц', 'месяц_текст', 'tracker_name'])
    .agg(
        сумма=('сумма', 'sum'),
        количество=('e-mail_оплаты', 'count')  # здесь считаем каждую запись как один повтор
    )
    .reset_index()
    .sort_values(by=['год', 'месяц', 'сумма'], ascending=[False, False, False])
)

for _, row in repeat_stats.iterrows():
    print(f"{row['месяц_текст']} {row['год']} — {row['tracker_name']}: {row['количество']} повторных платежей, {row['сумма']}₽")


# In[ ]:




