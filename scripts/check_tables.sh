#!/bin/bash

echo "🔍 Перевірка таблиць у базі даних..."

# Отримання назви контейнера MySQL
MYSQL_CONTAINER=$(docker ps --filter "ancestor=mysql:8" --format "{{.Names}}" | head -1)

if [ -z "$MYSQL_CONTAINER" ]; then
    echo "❌ Контейнер MySQL не знайдено. Спочатку запустіть ./run_etl.sh"
    exit 1
fi

echo "📦 Використовуємо контейнер: $MYSQL_CONTAINER"

# Функція для виконання SQL запитів
run_query() {
    local query="$1"
    local table_name="$2"

    echo ""
    echo "=== $table_name ==="
    docker exec $MYSQL_CONTAINER mysql -u analytics_user -panalytics_password advertising -e "$query" 2>/dev/null || echo "❌ Помилка виконання запиту для $table_name"
}

# Перевірка доступності MySQL
if ! docker exec $MYSQL_CONTAINER mysqladmin ping -h localhost -u analytics_user -panalytics_password --silent 2>/dev/null; then
    echo "❌ MySQL недоступний. Спочатку запустіть ./run_etl.sh"
    exit 1
fi

# Перевірка всіх таблиць
run_query "SELECT * FROM advertisers LIMIT 10;" "ADVERTISERS"
run_query "SELECT * FROM campaigns LIMIT 10;" "CAMPAIGNS"
run_query "SELECT * FROM users LIMIT 10;" "USERS"
run_query "SELECT * FROM user_interests LIMIT 10;" "USER_INTERESTS"
run_query "SELECT * FROM ad_events LIMIT 10;" "AD_EVENTS"
run_query "SELECT * FROM clicks LIMIT 10;" "CLICKS"

# Додаткова статистика
echo ""
echo "=== СТАТИСТИКА ТАБЛИЦЬ ==="
docker exec $MYSQL_CONTAINER mysql -u analytics_user -panalytics_password advertising -e "
SELECT
    'advertisers' as table_name, COUNT(*) as record_count FROM advertisers
UNION ALL
SELECT
    'campaigns' as table_name, COUNT(*) as record_count FROM campaigns
UNION ALL
SELECT
    'users' as table_name, COUNT(*) as record_count FROM users
UNION ALL
SELECT
    'user_interests' as table_name, COUNT(*) as record_count FROM user_interests
UNION ALL
SELECT
    'ad_events' as table_name, COUNT(*) as record_count FROM ad_events
UNION ALL
SELECT
    'clicks' as table_name, COUNT(*) as record_count FROM clicks;
"

echo ""
echo "✅ Перевірка завершена!"


