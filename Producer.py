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
            'Warszawa': {'min': 15, 'max': 25, 'city_name': 'Warszawa'},
            'Krakow': {'min': 14, 'max': 24, 'city_name': 'Krakow'},
            'Gdansk': {'min': 12, 'max': 22, 'city_name': 'Gdansk'},
            'Wroclaw': {'min': 16, 'max': 26, 'city_name': 'Wroclaw'},
            'Poznan': {'min': 15, 'max': 25, 'city_name': 'Poznan'},
            'Lodz': {'min': 14, 'max': 24, 'city_name': 'Lodz'},
        }

        # Inicjalizuj tracking ostatnich temperatur dla spike detection
        for city_name in self.cities.keys():
            setattr(self, f"last_temp_{city_name}", 20.0)

        # Topic Kafka
        self.topic = 'city-temperatures'

        print(f"🌡️  Temperature Sensor Simulator uruchomiony!")
        print(f"📡 Wysyłanie danych do topicu: {self.topic}")
        print(f"🏙️  Monitorowane miasta: {', '.join(self.cities.keys())}")
        print("-" * 60)

    def generate_temperature(self, city_config, city_name=None):
        """
        Generuje realistyczną temperaturę z różnymi typami anomalii
        2.5% - wysoka temperatura (50+°C)
        2.5% - niska temperatura (-30°C)
        5% - spike (nagły skok względem poprzedniej)
        """
        # Normalna temperatura bazowa
        base_temp = random.uniform(city_config['min'], city_config['max'])

        # Sprawdź typ anomalii
        anomaly_roll = random.random()

        if anomaly_roll < 0.025:  # 2.5% - Ekstremalna wysoka temperatura
            temp = random.uniform(50, 70)  # 50-70°C (usterka czujnika/pożar)
            print(f"🔥 ANOMALIA WYSOKA: Ekstremalna temperatura {temp:.1f}°C!")

        elif anomaly_roll < 0.05:  # 2.5% - Ekstremalna niska temperatura
            temp = random.uniform(-35, -20)  # -35 do -20°C (awaria/mróz)
            print(f"❄️  ANOMALIA NISKA: Ekstremalna temperatura {temp:.1f}°C!")

        elif anomaly_roll < 0.1:  # 5% - Spike (nagły skok)
            # Pobierz ostatnią temperaturę dla tego miasta
            if city_name is None:
                city_name = city_config.get('city_name', 'unknown')

            last_temp_key = f"last_temp_{city_name}"
            last_temp = getattr(self, last_temp_key, base_temp)

            # Nagły skok w górę lub w dół
            spike_direction = random.choice([1, -1])
            spike_magnitude = random.uniform(20, 40)  # 20-40°C skok
            temp = last_temp + (spike_direction * spike_magnitude)

            print(f"⚡ SPIKE ANOMALIA: {'+' if spike_direction > 0 else ''}{spike_direction * spike_magnitude:.1f}°C "
                  f"(z {last_temp:.1f}°C → {temp:.1f}°C)")

        else:  # 90% - Normalna temperatura
            temp = base_temp
            # Dodaj małe naturalne wahania
            temp += random.uniform(-2, 2)

        # Zapamiętaj temperaturę dla spike detection
        if city_name is None:
            city_name = city_config.get('city_name', 'unknown')
        setattr(self, f"last_temp_{city_name}", temp)

        return round(temp, 1)  # ← TO BYŁO NAJWAŻNIEJSZE!

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
                for city_name, config in self.cities.items():
                    # Generuj temperaturę z nowymi anomaliami (przekaż city_name explicite)
                    temperature = self.generate_temperature(config, city_name)

                    # Utwórz dane czujnika
                    sensor_data = self.create_sensor_data(city_name, temperature)

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

# main
if __name__ == "__main__":
    main()