import pandas as pd
import mysql.connector
from mysql.connector import Error


def etl_1000_events_fixed():
    mysql_config = {
        'host': 'mysql',
        'port': 3306,
        'user': 'analytics_user',
        'password': 'analytics_password',
        'database': 'advertising'
    }

    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        print("🚀 Завантаження перших 1000 подій...")

        # ОЧИЩЕННЯ ТАБЛИЦЬ (для повторного запуску)
        print("🧹 Очищення існуючих даних...")
        tables_to_clear = ['clicks', 'ad_events', 'campaigns', 'user_interests', 'users', 'advertisers']
        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")
        connection.commit()

        # 1. Завантажуємо тільки перші 1000 рядків з файлів
        events = pd.read_csv('data/ad_events.csv', quotechar='"', dtype=str, index_col=False, nrows=1000)
        campaigns = pd.read_csv('data/campaigns.csv')
        users = pd.read_csv('data/users.csv')

        print(f"📊 Завантажено: {len(events)} подій, {len(campaigns)} кампаній, {len(users)} користувачів")

        # ДІАГНОСТИКА
        print("\n=== ДІАГНОСТИКА ===")
        print("Колонки в events:", events.columns.tolist())
        print("Перші рекламодавці в events:", events['AdvertiserName'].unique()[:3])
        print("Перші рекламодавці в campaigns:", campaigns['AdvertiserName'].unique()[:3])

        # 2. ОБРОБКА РЕКЛАМОДАВЦІВ
        print("\n🏢 Обробка рекламодавців...")

        # Оборобка назв рекламодавців
        advertisers_from_events = set(events['AdvertiserName'].dropna().unique())
        advertisers_from_campaigns = set(campaigns['AdvertiserName'].dropna().unique())
        all_advertisers = advertisers_from_events.union(advertisers_from_campaigns)

        # Фільтруємо тільки правильні назви (що починаються з "Advertiser_")
        valid_advertisers = [adv for adv in all_advertisers
                             if isinstance(adv, str) and adv.startswith('Advertiser_')]

        print(f"Знайдено {len(valid_advertisers)} валідних рекламодавців:")
        print("Перші 5:", valid_advertisers[:5])

        cursor.executemany(
            "INSERT INTO advertisers (name) VALUES (%s)",
            [(adv,) for adv in valid_advertisers]
        )
        connection.commit()

        # Створюємо map для advertiser_id
        cursor.execute("SELECT advertiser_id, name FROM advertisers")
        adv_map = {name: aid for aid, name in cursor.fetchall()}
        print(f"✅ Створено {len(adv_map)} рекламодавців")

        # Перевірка що рекламодавці правильні
        cursor.execute("SELECT * FROM advertisers LIMIT 5")
        print("Перші рекламодавці в БД:", cursor.fetchall())

        # 3. Користувачі
        print("\n👥 Обробка користувачів...")
        user_rows = []
        user_interest_rows = []

        for _, row in users.iterrows():
            user_rows.append((
                int(row['UserID']),
                int(row['Age']) if not pd.isna(row['Age']) else None,
                row['Gender'] if not pd.isna(row['Gender']) else None,
                row['Location'] if not pd.isna(row['Location']) else None,
                row['SignupDate'] if not pd.isna(row['SignupDate']) else None
            ))

            # Обробка інтересів
            if pd.notna(row['Interests']):
                for interest in str(row['Interests']).split(','):
                    if interest.strip():
                        user_interest_rows.append((int(row['UserID']), interest.strip()))

        cursor.executemany(
            "INSERT INTO users (user_id, age, gender, location, signup_date) VALUES (%s, %s, %s, %s, %s)",
            user_rows
        )

        if user_interest_rows:
            cursor.executemany(
                "INSERT INTO user_interests (user_id, interest) VALUES (%s, %s)",
                user_interest_rows
            )
        connection.commit()
        print(f"✅ Створено {len(user_rows)} користувачів та {len(user_interest_rows)} інтересів")

        # 4. Кампанії (з campaigns.csv)
        print("\n📢 Обробка кампаній...")

        campaign_rows = []
        for _, row in campaigns.iterrows():
            advertiser_name = row['AdvertiserName']
            if advertiser_name in adv_map:
                campaign_rows.append((
                    row['CampaignName'],
                    adv_map[advertiser_name],
                    row['CampaignStartDate'],
                    row['CampaignEndDate'],
                    row['TargetingCriteria'],
                    row['AdSlotSize'],
                    float(row['Budget']),
                    float(row['RemainingBudget'])
                ))

        cursor.executemany("""
                           INSERT INTO campaigns
                           (name, advertiser_id, start_date, end_date, targeting_criteria, ad_slot_size, budget,
                            remaining_budget)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           """, campaign_rows)
        connection.commit()

        # Створюємо map для campaign_id
        cursor.execute("""
                       SELECT c.campaign_id, c.name as campaign_name, a.name as advertiser_name
                       FROM campaigns c
                                JOIN advertisers a ON c.advertiser_id = a.advertiser_id
                       """)
        camp_map = {f"{adv_name}_{camp_name}": camp_id for camp_id, camp_name, adv_name in cursor.fetchall()}
        print(f"✅ Створено {len(campaign_rows)} кампаній, mapping: {len(camp_map)}")

        # 5. Події (ad_events) та кліки (clicks)
        print("\n📺 Обробка подій...")
        ad_events_rows = []
        clicks_rows = []
        matched_events = 0

        for _, row in events.iterrows():
            advertiser_name = row['AdvertiserName']
            campaign_name = row['CampaignName']
            camp_key = f"{advertiser_name}_{campaign_name}"

            if camp_key in camp_map and not pd.isna(row['UserID']):
                campaign_id = camp_map[camp_key]

                ad_events_rows.append((
                    row['EventID'],
                    campaign_id,
                    int(row['UserID']),
                    row['Timestamp'] if not pd.isna(row['Timestamp']) else '2024-01-01 00:00:00',
                    row['AdSlotSize'] if not pd.isna(row['AdSlotSize']) else '300x250',
                    row['Device'] if not pd.isna(row['Device']) else 'Desktop',
                    row['Location'] if not pd.isna(row['Location']) else 'Unknown',
                    float(row['BidAmount']) if not pd.isna(row['BidAmount']) else 0.0,
                    float(row['AdCost']) if not pd.isna(row['AdCost']) else 0.0
                ))
                matched_events += 1

                # Якщо був клік
                if (row.get('WasClicked', False) == True and
                        not pd.isna(row.get('ClickTimestamp'))):
                    clicks_rows.append((
                        row['EventID'],
                        row['ClickTimestamp'],
                        float(row['AdRevenue']) if not pd.isna(row['AdRevenue']) else 0.0
                    ))

        print(f"📊 Знайдено відповідність для {matched_events} з {len(events)} подій")

        # Вставка подій
        if ad_events_rows:
            cursor.executemany("""
                               INSERT INTO ad_events
                               (event_id, campaign_id, user_id, timestamp, ad_slot_size, device, location, bid_amount,
                                ad_cost)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                               """, ad_events_rows)
            print(f"✅ Вставлено {len(ad_events_rows)} подій")
        else:
            print("❌ Немає подій для вставки!")

        # Вставка кліків
        if clicks_rows:
            cursor.executemany("""
                               INSERT INTO clicks
                                   (event_id, click_timestamp, ad_revenue)
                               VALUES (%s, %s, %s)
                               """, clicks_rows)
            print(f"✅ Вставлено {len(clicks_rows)} кліків")
        else:
            print("ℹ️ Немає кліків у цих подіях")

        connection.commit()

        # Фінальна перевірка
        print("\n=== ФІНАЛЬНА СТАТИСТИКА ===")
        tables = ['advertisers', 'campaigns', 'users', 'user_interests', 'ad_events', 'clicks']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"📊 {table}: {count:,} записів")

        print("\n✅ ETL завершено успішно!")

    except Error as e:
        print(f"❌ Помилка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    etl_1000_events_fixed()











