#!/bin/bash

echo "🚀 Запуск ETL конвеєра..."

# Повне очищення Docker
echo "🧹 Повне очищення Docker контейнерів..."
docker stop $(docker ps -aq) 2>/dev/null || true
docker rm $(docker ps -aq) 2>/dev/null || true
docker-compose down -v --remove-orphans 2>/dev/null || true

# Запуск MySQL
echo "📊 Запуск MySQL..."
docker-compose up -d mysql

# Отримання реальної назви контейнера
echo "🔍 Пошук контейнера MySQL..."
MYSQL_CONTAINER=$(docker ps --filter "ancestor=mysql:8" --format "{{.Names}}" | head -1)

if [ -z "$MYSQL_CONTAINER" ]; then
    echo "❌ Контейнер MySQL не знайдено"
    exit 1
fi

echo "📦 Знайдено контейнер: $MYSQL_CONTAINER"

# Очікування готовності MySQL
echo "⏳ Очікування готовності MySQL..."
timeout=60
counter=0
until docker exec $MYSQL_CONTAINER mysqladmin ping -h localhost -u analytics_user -panalytics_password --silent 2>/dev/null; do
    if [ $counter -ge $timeout ]; then
        echo "❌ Timeout: MySQL не готовий за $timeout секунд"
        docker logs $MYSQL_CONTAINER
        exit 1
    fi
    echo "Очікування MySQL... ($counter/$timeout)"
    sleep 2
    ((counter+=2))
done

echo "✅ MySQL готовий!"

# Запуск ETL
echo "🔄 Запуск ETL процесу..."
docker-compose --profile etl up etl_env

echo "✅ ETL завершено!"


