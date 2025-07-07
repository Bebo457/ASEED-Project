#!/usr/bin/env python3
"""
Spark Structured Streaming - Wykrywanie anomalii temperatury
Prosty i czytelny kod do analizy danych z czujników IoT
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# === KONFIGURACJA OKIEN CZASOWYCH ===
WINDOW_DURATION = "30 seconds"  # Długość okna dla agregacji
WATERMARK_DELAY = "1 minute"  # Opóźnienie watermark (powinno być >= WINDOW_DURATION)


def create_spark_session():
    """
    Tworzy sesję Spark z konfiguracją dla Kafka
    """
    return SparkSession.builder \
        .appName("TemperatureAnomalyDetector") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()


def setup_output_directories():
    """
    Tworzy katalogi na wyniki
    """
    directories = ["output/normal_data", "output/anomalies", "output/aggregations"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    print("📁 Katalogi wyjściowe utworzone")


def define_schema():
    """
    Definiuje schemat danych z czujników
    """
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True)
    ])


def detect_temperature_anomalies(df):
    """
    Wykrywa anomalie temperaturowe - PROSTE PROGI
    """
    return df.withColumn(
        "anomaly_type",
        when(col("temperature") > 35.0, "HIGH_TEMP")
        .when(col("temperature") < 0.0, "LOW_TEMP")
        .otherwise("NORMAL")
    ).withColumn(
        "is_anomaly",
        col("anomaly_type") != "NORMAL"
    )


def main():
    """
    Główna funkcja aplikacji
    """
    print("🚀 Uruchomienie Spark Anomaly Detector")
    print("=" * 50)

    # Utwórz sesję Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Mniej logów

    # Przygotuj katalogi
    setup_output_directories()

    # Schemat danych
    schema = define_schema()

    print("📡 Łączenie z Kafka...")

    # Odczyt strumienia z Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "city-temperatures") \
        .option("startingOffsets", "latest") \
        .load()

    print("✅ Połączono z Kafka")
    print("🔍 Rozpoczynam analizę danych...")

    # Parsowanie JSON z Kafka
    parsed_stream = kafka_stream \
        .select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ) \
        .select("data.*", "kafka_timestamp")

    # Konwersja timestamp na właściwy format
    processed_stream = parsed_stream \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("processing_time", current_timestamp())

    # WYKRYWANIE ANOMALII
    with_anomalies = detect_temperature_anomalies(processed_stream)

    # === ZAPIS 1: WSZYSTKIE DANE (z flagami anomalii) ===
    all_data_query = with_anomalies \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/normal_data") \
        .option("checkpointLocation", "checkpoints/all_data") \
        .trigger(processingTime="10 seconds") \
        .start()

    # === ZAPIS 2: TYLKO ANOMALIE ===
    anomalies_only = with_anomalies.filter(col("is_anomaly") == True)

    anomalies_query = anomalies_only \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/anomalies") \
        .option("checkpointLocation", "checkpoints/anomalies") \
        .trigger(processingTime="5 seconds") \
        .start()

    # === ZAPIS 3: AGREGACJE W OKNACH CZASOWYCH ===
    # Średnia temperatura co 30 sekund dla każdego miasta
    windowed_aggregates = with_anomalies \
        .withWatermark("timestamp", WATERMARK_DELAY) \
        .groupBy(
        window(col("timestamp"), WINDOW_DURATION),
        col("city")
    ) \
        .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        count("*").alias("measurement_count"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
    ) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    aggregates_query = windowed_aggregates \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/aggregations") \
        .option("checkpointLocation", "checkpoints/aggregations") \
        .trigger(processingTime="30 seconds") \
        .start()

    # === WYŚWIETLANIE NA KONSOLI ===
    # Pokaz wykryte anomalie w czasie rzeczywistym
    console_query = anomalies_only.select(
        "timestamp",
        "city",
        "temperature",
        "anomaly_type",
        "sensor_id"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("🎯 Aplikacja uruchomiona! Monitoruję anomalie...")
    print("📊 Dane zapisywane do:")
    print("   • output/normal_data/ - wszystkie pomiary")
    print("   • output/anomalies/ - tylko anomalie")
    print(f"   • output/aggregations/ - statystyki {WINDOW_DURATION}")
    print("💻 Anomalie wyświetlane na konsoli")
    print(f"⏰ Okno czasowe: {WINDOW_DURATION}")
    print("\n🛑 Ctrl+C aby zatrzymać")
    print("=" * 50)

    try:
        # Czekaj na wszystkie strumienie
        all_data_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Zatrzymywanie aplikacji...")

        # Zatrzymaj wszystkie strumienie
        all_data_query.stop()
        anomalies_query.stop()
        aggregates_query.stop()
        console_query.stop()

        print("✅ Aplikacja zatrzymana")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()