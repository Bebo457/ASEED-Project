#!/bin/bash
echo "🛑 Zatrzymywanie systemu monitorowania temperatury..."
docker-compose down

echo "🧹 Usuwanie kontenerów i sieci..."
docker-compose down --remove-orphans

echo "✅ System zatrzymany!"