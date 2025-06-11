import pandas as pd
import time
from datetime import datetime
import requests
from io import StringIO

# Пути к файлам
PAYMENTS_PATH = 'export_(ReportPayments.2025-02-14 - 2025-06-08.).csv'

# Авторизация и параметры AppMetrica
API_TOKEN = "y0__xDunbWlqveAAhianDcgvtvu8hI4Lgj1FE3Wx6z8be6gSyQ7sTrc4A"
APPLICATION_ID = "4661140"
DATE_SINCE = "2025-02-14 00:00:00"
DATE_UNTIL = datetime.now().strftime("%Y-%m-%d 00:00:00")

# Классификация переходов
def fetch_installations_csv():
    url = (
        f"https://api.appmetrica.yandex.ru/logs/v1/export/installations.csv?"
        f"application_id={APPLICATION_ID}"
        f"&date_since={DATE_SINCE.replace(' ', '%20')}"
        f"&date_until={DATE_UNTIL.replace(' ', '%20')}"
        f"&date_dimension=default"
        f"&fields=tracker_name,profile_id"
    )
    headers = {"Authorization": f"OAuth {API_TOKEN}"}
    
    print("Отправка запроса в AppMetrica...")
    for attempt in range(30):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Данные получены.")
            df = pd.read_csv(StringIO(response.text), header=0)
            return df
        elif response.status_code == 202:
            print(f"Попытка {attempt + 1}/30: данные ещё не готовы, ждём 10 сек...")
            time.sleep(10)
        else:
            raise Exception(f"Ошибка: {response.status_code}, {response.text}")
    
    raise TimeoutError("Данные не были готовы в течение 5 минут.")

def classify_transactions(group):
    group = group.sort_values('дата')
    types = []
    full_found = False
    for _, row in group.iterrows():
        is_auto = str(row['тип_платежа']).strip().lower() == 'автоплатеж'
        is_promo = row['промо'] == 1
        if is_promo:
            types.append('promo')
        elif not full_found and is_auto:
            types.append('full')
            full_found = True
        elif full_found and is_auto:
            types.append('repeat')
        else:
            types.append('other')
    group['тип_сделки'] = types
    return group

# Загрузка платежей
with open(PAYMENTS_PATH, 'r', encoding='utf-8') as f:
    lines = f.readlines()

header_line = lines[10].strip().split(';')
df = pd.read_csv(PAYMENTS_PATH, skiprows=11, sep=';', names=header_line)
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
df['дата'] = pd.to_datetime(df['дата'], errors='coerce')

# Фильтрация успешных платежей
df = df[
    (df['статус'].str.lower() == 'успешно') & 
    (df['e-mail_оплаты'].notnull())
].sort_values(by=['e-mail_оплаты', 'id_пакета', 'дата'])

df = df.groupby(['e-mail_оплаты', 'id_пакета']).apply(classify_transactions).reset_index(drop=True)

# Повторные платежи
repeat_df = df[df['тип_сделки'] == 'repeat'].copy()
df = df[df['тип_сделки'] == 'full']

# Загрузка трекеров через API
install_df = fetch_installations_csv()
install_df = install_df.dropna(subset=['profile_id'])
install_df = install_df.rename(columns={'profile_id': 'девайс_оплаты'})
install_df['tracker_name'] = install_df['tracker_name'].fillna('unknown')

df = df.merge(install_df, on='девайс_оплаты', how='left')
df['tracker_name'] = df['tracker_name'].fillna('unknown')
repeat_df = repeat_df.merge(install_df, on='девайс_оплаты', how='left')
repeat_df['tracker_name'] = repeat_df['tracker_name'].fillna('unknown')

# Добавление года и номера недели
for target_df in [df, repeat_df]:
    calendar = target_df['дата'].dt.isocalendar()
    target_df['год'] = calendar.year
    target_df['неделя'] = calendar.week
    target_df['неделя_диапазон'] = target_df['дата'].dt.to_period('W').astype(str)

# Создание строки пользователя
df['пользователь'] = df.apply(
    lambda row: f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽", axis=1
)

# Группировка по неделям и пакетам
grouped = (
    df.groupby(['год', 'неделя', 'неделя_диапазон', 'пакет'])
    .agg(переходы=('пользователь', lambda x: sorted(set(x))))
    .reset_index()
    .sort_values(by=['год', 'неделя'], ascending=False)
)

print("Переходы по неделям и пакетам:\n")
total = 0
for _, row in grouped.iterrows():
    count = len(row['переходы'])
    total += count
    print(f"{row['неделя_диапазон']} — {row['пакет']}: {count}")
print(f"\nВсего переходов: {total}\n")

for _, row in grouped.iterrows():
    print(f"{row['неделя_диапазон']} — {row['пакет']}:")
    for u in row['переходы']:
        print(f"  {u}")
    print()

# Уникальные комбинации для корректной статистики
df['уникальная_комбинация'] = df.apply(
    lambda row: f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽", axis=1
)
repeat_df['уникальная_комбинация'] = repeat_df.apply(
    lambda row: f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽", axis=1
)

unique_full = df.drop_duplicates(subset=['уникальная_комбинация'])

# Статистика по трекерам (FULL)
print("\nСтатистика по трекерам (full) по неделям:\n")
weekly_tracker_stats = (
    unique_full.groupby(['год', 'неделя', 'неделя_диапазон', 'tracker_name'])
    .agg(
        сумма=('сумма', 'sum'),
        количество=('уникальная_комбинация', 'count')
    )
    .reset_index()
    .sort_values(by=['год', 'неделя', 'сумма'], ascending=[False, False, False])
)

for _, row in weekly_tracker_stats.iterrows():
    print(f"{row['неделя_диапазон']} — {row['tracker_name']}: {row['количество']} пользователей, {row['сумма']}₽")

# Статистика по повторным платежам (REPEAT) с учётом уникальности в пределах недели
print("\nСтатистика по повторным платежам (repeat) по неделям и трекерам:\n")
unique_repeat = repeat_df.drop_duplicates(
    subset=['год', 'неделя', 'tracker_name', 'уникальная_комбинация']
)

repeat_stats = (
    unique_repeat.groupby(['год', 'неделя', 'неделя_диапазон', 'tracker_name'])
    .agg(
        сумма=('сумма', 'sum'),
        количество=('уникальная_комбинация', 'count')
    )
    .reset_index()
    .sort_values(by=['год', 'неделя', 'сумма'], ascending=[False, False, False])
)

for _, row in repeat_stats.iterrows():
    print(f"{row['неделя_диапазон']} — {row['tracker_name']}: {row['количество']} повторных платежей, {row['сумма']}₽")

# Повторные платежи по неделям — детальный список
print("\nПовторные платежи с разбивкой по неделям:\n")
repeat_df_sorted = repeat_df.sort_values(by=['год', 'неделя', 'дата'])
repeat_grouped = repeat_df_sorted.groupby('неделя_диапазон')

for week_range, group in repeat_grouped:
    count = group.shape[0]
    print(f"{week_range} — [{count}]:")
    for _, row in group.iterrows():
        print(f"{row['e-mail_оплаты']} — {row['девайс_оплаты']} — [{row['tracker_name']}] — {row['сумма']}₽")
    print()