# DE_HW1
# 📊 Advertising Analytics ETL Pipeline

## 🧾 Опис завдання

Цей проєкт реалізує повний цикл **ETL-процесу** для нормалізації денормалізованого рекламного датасету. Метою є усунення надмірності, забезпечення цілісності даних і ефективність запитів, таких як розрахунок CTR або загальних витрат рекламодавців.

---

## 🗂️ Структура репозиторію

```bash
.
├── data/                        # Початкові CSV-файли (events, campaigns, users - відстутні у даному репозиторії через обмеження розміру)
├── init/                         
│   ├── init_db.sql              # SQL-скрипти створення таблиц
├── scripts/                         
│   └── env_etl.sh               # 🔄 Shell-скрипт для запуску mysql бази, завантаження бібліотек для обробки датасету для etl,запуску ETL - процесу
│   └── check_tables.sh          # 🔍 Shell-скрипт для перевірки, чи створені всі таблиці (через SELECT COUNT та вивід 10 семплів таблиці)
├── screenshots/                 # Скріни результатів запитів
│   └── table_previews.png
├── docker-compose.yml       # докер
├── .gitignore
├── etl_transform.py
└── README.md                    
```

---

## ⚙️ Компоненти проєкту

### ✅ 1. Нормалізована реляційна схема (3NF)

Модель включає 6 таблиць:

| Таблиця           | Призначення |
|------------------|-------------|
| `advertisers`     | Рекламодавці (унікальні) |
| `campaigns`       | Кампанії, пов’язані з рекламодавцями |
| `users`           | Інформація про користувачів |
| `user_interests`  | Багатозначна зв’язка user → interest |
| `ad_events`       | Події показу реклами |
| `clicks`          | Події кліку, пов’язані через `event_id` |

➡ **Чому так?**
- Забезпечено **розділення сутностей**
- `clicks.event_id` → `ad_events.event_id` забезпечує коректний зв’язок показу і кліку
- Спрощено обчислення CTR: `SELECT COUNT(*) FROM clicks WHERE campaign_id = ?` проти `ad_events`

DDL скрипти: `init/init_db.sql`

---

### ✅ 2. ETL-скрипт

```bash
python etl_trasnform.py
```

🔹 Робить наступне:
- Завантажує 1000 подій + усі кампанії та користувачів
- Очищує таблиці для повторного запуску
- Перевіряє валідність рекламодавців (`Advertiser_`)
- Формує `adv_map` та `camp_map` для відповідності
- Вставляє дані в нормалізовані таблиці (включаючи `user_interests` та `clicks`)

---

### ✅ 3. Docker-середовище

⬇️ Створює:

🐬 mysql: Контейнер з MySQL 8.0

-Автоматично створює БД advertising
-Користувач analytics_user з паролем analytics_password
-Запускає всі скрипти з ./init/ при старті (наприклад, створення таблиць)

🐍 etl_env: Легкий Python-сервіс (на базі python:3.11-slim)
- Встановлює залежності з requirements.txt
- Виконує скрипт etl_transform.py після того, як MySQL готовий
- Використовує кеш pip для швидшої інсталяції
- Встановлює необхідні пакети системи (gcc, libmysqlclient-dev) для підключення до MySQL

---

### ✅ 4. Перевірка результатів

У директорії `screenshots/` збережені результати:

```sql
SELECT * FROM advertisers LIMIT 10;
SELECT * FROM campaigns LIMIT 10;
...
```

---

## 🚀 Швидкий запуск

1. 🔁 **Склонуйте репозиторій:**

```bash
git clone https://github.com/yourusername/DE_HW1_ETL.git
cd DE_HW1_ETL
```

2. 🐳 **Запустіть повний ETL-процес із Docker:**

```bash
bash scripts/run_etl.sh
```

Скрипт автоматично:
- Зупиняє старі Docker-контейнери
- Створює контейнер `mysql` і очікує його готовності
- Запускає ETL-пайплайн у контейнері `etl_env` за допомогою профілю Docker Compose

3. 🧪 **Перевірте структуру та вміст таблиць:**

```bash
bash scripts/check_tables.sh
```

Скрипт виконує:
- `SELECT * FROM ... LIMIT 10` для кожної таблиці
- Виводить кількість записів у всіх таблицях (`advertisers`, `campaigns`, `users`, `user_interests`, `ad_events`, `clicks`)

## 📷 Скріни

![Таблиці](screenshots/table_previews.png)

---

## 🔗 Посилання

> 📁 [GitHub репозиторій (публічний)](https://github.com/Alexdatamed/DE_HW1)

---

## 📬 Автор

**Олексій Самойленко**  
Clinical AI Engineer | SET University | Mark Wealth  
📧
