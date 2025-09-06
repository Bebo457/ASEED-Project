#!/usr/bin/env python3
import os
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

THRESHOLD_UP = 40
THRESHOLD_DOWN = -30
batch_id = 0


class TemperatureSensorSimulator:
    def __init__(self):
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=60000,
            retry_backoff_ms=1000
        )

        self.cities = ['Warszawa', 'Krakow', 'Gdansk', 'Wroclaw', 'Poznan', 'Lodz']
        self.topic = 'city-temperatures'

    def generate_temperature(self, city):
        base_temp = random.uniform(15, 25)
        anomaly = random.random()

        if anomaly < 0.1:
            temp = random.uniform(THRESHOLD_UP, THRESHOLD_UP + 20)
            print(f"{city} {round(temp, 2)}°C ---ANOMALY HOT--- ")
        elif anomaly < 0.2:
            temp = random.uniform(THRESHOLD_DOWN - 20, THRESHOLD_DOWN)
            print(f"{city} {round(temp, 2)}°C ---ANOMALY COLD--- ")
        else:
            temp = base_temp + random.uniform(-5, 5)
            print(f"{city} {round(temp, 2)}°C")

        return round(temp, 1)

    def create_data(self, city, temperature, batch_id):
        return {
            'batch_id': batch_id,
            'city': city,
            'temperature': temperature,
            'timestamp': datetime.now().isoformat()
        }

    def send_data(self, data):
        self.producer.send(self.topic, data)

    def run(self, duration_minutes, interval_seconds):
        global batch_id
        print(f"Time of the simulation: {duration_minutes}. Frequency {interval_seconds}s")
        print("-" * 50)

        end_time = time.time() + (duration_minutes * 60)

        try:
            while time.time() < end_time:
                batch_id += 1
                print(f"Data ID: {batch_id}")

                for city in self.cities:
                    temperature = self.generate_temperature(city)
                    data = self.create_data(city, temperature, batch_id)
                    self.send_data(data)

                print("-" * 50)
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n Stopped")
        finally:
            self.producer.close()


if __name__ == "__main__":
    print("🌡️ Temperature Sensor Simulator")

    duration = int(os.getenv('SIMULATION_DURATION', '60'))
    interval = int(os.getenv('SIMULATION_INTERVAL', '5'))

    print(f"Time of the simulation: {duration} minutes")
    print(f"Frequency: {interval} seconds")

    try:
        simulator = TemperatureSensorSimulator()
        simulator.run(duration, interval)
    except KeyboardInterrupt:
        print("\nEnd of the simulation")