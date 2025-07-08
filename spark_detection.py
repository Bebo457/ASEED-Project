#!/usr/bin/env python3
"""
Spark Structured Streaming - Wykrywanie anomalii temperatury
Rate-based spike detection + zwykłe progi
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# === KONFIGURACJA PROGÓW ANOMALII ===
TEMP_HIGH_THRESHOLD = 35.0      # °C - bardzo gorąco
TEMP_LOW_THRESHOLD = -10.0      # °C - bardzo zimno

# === KONFIGURACJA OKIEN CZASOWYCH ===
WINDOW_DURATION = "30 seconds"  # Długość okna dla agregacji
WATERMARK_DELAY = "1 minute"    # Opóźnienie watermark


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


def detect_temperature_anomalies_simple(df):
    """
    Wykrywa anomalie: tylko progi absolutne (streaming-compatible)
    """
    return df.withColumn(
        "anomaly_type",
        when(col("temperature") > TEMP_HIGH_THRESHOLD, "HIGH_TEMP")
        .when(col("temperature") < TEMP_LOW_THRESHOLD, "LOW_TEMP")
        .otherwise("NORMAL")
    ).withColumn(
        "is_anomaly",
        col("anomaly_type") != "NORMAL"
    ).withColumn(
        "anomaly_details",
        when(col("anomaly_type") == "HIGH_TEMP",
             concat(lit("Very hot: "), round(col("temperature"), 1), lit("°C")))
        .when(col("anomaly_type") == "LOW_TEMP",
             concat(lit("Very cold: "), round(col("temperature"), 1), lit("°C")))
        .otherwise("Normal reading")
    )


def main():
    """
    Główna funkcja aplikacji
    """
    print("🚀 Uruchomienie Spark Anomaly Detector")
    print("=" * 50)
    print(f"🌡️  Progi temperatur: {TEMP_LOW_THRESHOLD}°C < NORMAL < {TEMP_HIGH_THRESHOLD}°C")
    print("📊 Wykrywanie: progi absolutne (HIGH_TEMP, LOW_TEMP)")
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

    # WYKRYWANIE ANOMALII (bez spike detection - streaming limitation)
    with_anomalies = detect_temperature_anomalies_simple(processed_stream)

    # === ZAPIS 1: WSZYSTKIE DANE (z flagami anomalii) - CSV ===
    all_data_query = with_anomalies \
        .select(
        "sensor_id", "city", "temperature", "timestamp", "humidity", "pressure",
        "processing_time", "anomaly_type", "is_anomaly", "anomaly_details"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "output/normal_data") \
        .option("checkpointLocation", "checkpoints/all_data") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("encoding", "UTF-8") \
        .trigger(processingTime="10 seconds") \
        .start()

    # === ZAPIS 2: TYLKO ANOMALIE - CSV ===
    anomalies_only = with_anomalies.filter(col("is_anomaly") == True)

    anomalies_query = anomalies_only \
        .select(
        "sensor_id", "city", "temperature", "timestamp",
        "anomaly_type", "anomaly_details"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "output/anomalies") \
        .option("checkpointLocation", "checkpoints/anomalies") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("encoding", "UTF-8") \
        .trigger(processingTime="5 seconds") \
        .start()

    # === ZAPIS 3: AGREGACJE W OKNACH CZASOWYCH - CSV ===
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
        .drop("window") \
        .select(
        "window_start", "window_end", "city", "avg_temperature",
        "min_temperature", "max_temperature", "measurement_count",
        "anomaly_count"
    )

    aggregates_query = windowed_aggregates \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", "output/aggregations") \
        .option("checkpointLocation", "checkpoints/aggregations") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("encoding", "UTF-8") \
        .trigger(processingTime="30 seconds") \
        .start()

    # === WYŚWIETLANIE NA KONSOLI ===
    console_query = anomalies_only.select(
        "timestamp",
        "city",
        "temperature",
        "anomaly_type",
        "anomaly_details"
    ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("🎯 Aplikacja uruchomiona! Monitoruję anomalie...")
    print("📊 Dane zapisywane do CSV:")
    print("   • output/normal_data/ - wszystkie pomiary + anomaly detection")
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