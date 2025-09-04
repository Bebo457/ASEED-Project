#!/bin/bash
echo "🚀 Uruchamianie systemu monitorowania temperatury"
echo "================================================"

# Sprawdzenie czy Docker i Docker Compose są zainstalowane
if ! command -v docker &> /dev/null; then
    echo "❌ Docker nie jest zainstalowany!"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose nie jest zainstalowany!"
    exit 1
fi

# Tworzenie katalogów wyjściowych na hoście
mkdir -p output/normal_data output/anomalies output/aggregations checkpoints

echo "📦 Budowanie obrazów Docker..."
docker-compose build

echo "🔧 Uruchamianie infrastruktury (Zookeeper + Kafka)..."
docker-compose up -d zookeeper kafka kafka-init

echo "⏳ Oczekiwanie na gotowość Kafka..."
sleep 30

echo "🌡️ Uruchamianie producenta temperatury..."
docker-compose up -d temperature-producer

echo "⏳ Oczekiwanie na dane..."
sleep 10

echo "⚡ Uruchamianie detektora anomalii Spark..."
docker-compose up -d spark-detector

echo "🖥️ Uruchamianie Kafka UI..."
docker-compose up -d kafka-ui

echo ""
echo "✅ System uruchomiony!"
echo "================================================"
echo "📊 Kafka UI: http://localhost:8080"
echo "🔥 Spark UI: http://localhost:4040"
echo "📁 Wyniki w katalogu: ./output/"
echo "📋 Logi: docker-compose logs -f [service-name]"
echo ""
echo "Aby zatrzymać system: ./stop.sh"
echo "Aby zobaczyć logi: docker-compose logs -f"
