#!/usr/bin/env python3
"""
IoT Temperature Sensor Simulator
Symuluje czujniki temperatury w różnych miastach Polski
Wysyła dane do Kafka w czasie rzeczywistym
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


class TemperatureSensorSimulator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """
        Inicjalizacja symulatora czujników
        """
        # Konfiguracja Kafka z poprawnym kodowaniem UTF-8
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )

        # Miasta i ich typowe temperatury (średnie sezonowe)
        self.cities = {
            'Warszawa': {'min': 15, 'max': 25, 'anomaly_chance': 0.05},
            'Krakow': {'min': 14, 'max': 24, 'anomaly_chance': 0.05},
            'Gdansk': {'min': 12, 'max': 22, 'anomaly_chance': 0.05},
            'Wroclaw': {'min': 16, 'max': 26, 'anomaly_chance': 0.05},
            'Poznan': {'min': 15, 'max': 25, 'anomaly_chance': 0.05},
            'Lodz': {'min': 14, 'max': 24, 'anomaly_chance': 0.05},
        }

        # Topic Kafka
        self.topic = 'city-temperatures'

        print(f"🌡️  Temperature Sensor Simulator uruchomiony!")
        print(f"📡 Wysyłanie danych do topicu: {self.topic}")
        print(f"🏙️  Monitorowane miasta: {', '.join(self.cities.keys())}")
        print("-" * 60)

    def generate_temperature(self, city_config):
        """
        Generuje realistyczną temperaturę dla miasta
        """
        # Normalna temperatura
        temp = random.uniform(city_config['min'], city_config['max'])

        # Czasami dodaj anomalie
        if random.random() < city_config['anomaly_chance']:
            if random.choice([True, False]):
                # Anomalia - bardzo gorąco
                temp += random.uniform(15, 25)
                print(f"🔥 ANOMALIA: Bardzo wysoka temperatura!")
            else:
                # Anomalia - bardzo zimno
                temp -= random.uniform(10, 20)
                print(f"❄️  ANOMALIA: Bardzo niska temperatura!")

        # Dodaj małe losowe wahania
        temp += random.uniform(-2, 2)

        return round(temp, 1)

    def create_sensor_data(self, city, temperature):
        """
        Tworzy strukturę danych czujnika
        """
        return {
            'sensor_id': f"sensor_{city.lower()}_{random.randint(1, 3)}",
            'city': city,
            'temperature': temperature,
            'timestamp': datetime.now().isoformat(),
            'humidity': round(random.uniform(30, 70), 1),  # Bonus: wilgotność
            'pressure': round(random.uniform(1000, 1030), 1)  # Bonus: ciśnienie
        }

    def send_data(self, data):
        """
        Wysyła dane do Kafka
        """
        try:
            future = self.producer.send(self.topic, data)
            # Czekaj na potwierdzenie
            record_metadata = future.get(timeout=10)

            # Log wysłanych danych
            temp_status = "🔥" if data['temperature'] > 30 else "❄️" if data['temperature'] < 0 else "🌡️"
            print(f"{temp_status} {data['city']}: {data['temperature']}°C | "
                  f"Czujnik: {data['sensor_id']} | {data['timestamp'][:19]}")

            return True
        except Exception as e:
            print(f"❌ Błąd wysyłania danych: {e}")
            return False

    def run_simulation(self, duration_minutes=60, interval_seconds=5):
        """
        Uruchamia symulację na określony czas

        Args:
            duration_minutes: czas trwania symulacji w minutach
            interval_seconds: odstęp między wysyłaniem danych w sekundach
        """
        print(f"▶️  Rozpoczynam symulację na {duration_minutes} minut")
        print(f"⏱️  Dane wysyłane co {interval_seconds} sekund")
        print("=" * 60)

        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)

        try:
            while time.time() < end_time:
                # Dla każdego miasta
                for city, config in self.cities.items():
                    # Generuj temperaturę
                    temperature = self.generate_temperature(config)

                    # Utwórz dane czujnika
                    sensor_data = self.create_sensor_data(city, temperature)

                    # Wyślij do Kafka
                    self.send_data(sensor_data)

                # Pauza przed następnym cyklem
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n⏹️  Symulacja zatrzymana przez użytkownika")
        except Exception as e:
            print(f"\n❌ Nieoczekiwany błąd: {e}")
        finally:
            self.producer.close()
            print("🔚 Symulacja zakończona")

    def send_test_data(self, count=10):
        """
        Wysyła testowe dane (dla szybkiego testu)
        """
        print(f"🧪 Wysyłanie {count} testowych pomiarów...")

        for i in range(count):
            city = random.choice(list(self.cities.keys()))
            config = self.cities[city]
            temperature = self.generate_temperature(config)
            sensor_data = self.create_sensor_data(city, temperature)

            if self.send_data(sensor_data):
                time.sleep(1)

        print("✅ Test zakończony")


def main():
    """
    Główna funkcja uruchamiająca symulator
    """
    print("🚀 IoT Temperature Sensor Simulator")
    print("=" * 40)

    # Utwórz symulator
    simulator = TemperatureSensorSimulator()

    while True:
        print("\n📋 Wybierz opcję:")
        print("1. Uruchom symulację (ciągłe wysyłanie)")
        print("2. Wyślij testowe dane (10 pomiarów)")
        print("3. Konfiguracja zaawansowana")
        print("4. Zakończ")

        choice = input("\nTwój wybór (1-4): ").strip()

        if choice == '1':
            try:
                duration = int(input("Czas symulacji w minutach (domyślnie 60): ") or 60)
                interval = int(input("Odstęp między danymi w sekundach (domyślnie 5): ") or 5)
                simulator.run_simulation(duration, interval)
            except ValueError:
                print("❌ Nieprawidłowa wartość liczbowa")

        elif choice == '2':
            simulator.send_test_data()

        elif choice == '3':
            print("\n🔧 Konfiguracja zaawansowana:")
            print("Obecnie niedostępna - edytuj kod aby zmienić miasta lub parametry")

        elif choice == '4':
            print("👋 Do widzenia!")
            break

        else:
            print("❌ Nieprawidłowy wybór")


if __name__ == "__main__":
    main()