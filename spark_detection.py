#!/usr/bin/env python3
"""
Spark Structured Streaming - Temperature anomaly detection
Threshold-based anomaly detection for IoT temperature sensors
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Anomaly detection thresholds
TEMP_HIGH_THRESHOLD = 40  # °C - very hot
TEMP_LOW_THRESHOLD = -30  # °C - very cold

# Time window configuration
WINDOW_DURATION = "30 seconds"  # Aggregation window length
WATERMARK_DELAY = "1 minute"  # Late data tolerance

# Processing frequency configuration
PROCESSING_INTERVAL = "5 seconds"  # How often to process and save data


def create_spark_session():
    """
    Creates Spark session with Kafka integration
    """
    return SparkSession.builder \
        .appName("TemperatureAnomalyDetector") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
        .getOrCreate()


def setup_output_directories():
    """
    Creates output directories, removes old ones if they exist
    """
    import shutil

    directories = ["output/normal_data", "output/anomalies", "output/aggregations", "checkpoints"]
    for directory in directories:
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.makedirs(directory, exist_ok=True)
    print("📁 Output directories created (old ones removed)")


def define_schema():
    """
    Defines JSON schema for sensor data from Kafka
    """
    return StructType([
        StructField("batch_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ])


def detect_temperature_anomalies(df):
    """
    Detects temperature anomalies using threshold-based approach
    Adds anomaly flags and details to the dataframe
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
    Main application function - sets up streaming pipeline
    """
    print("🚀 Starting Spark Anomaly Detector")
    print("=" * 50)
    print(f"🌡️  Temperature thresholds: {TEMP_LOW_THRESHOLD}°C < NORMAL < {TEMP_HIGH_THRESHOLD}°C")
    print("📊 Detection method: absolute thresholds (HIGH_TEMP, LOW_TEMP)")
    print("=" * 50)

    # Initialize Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Prepare output directories
    setup_output_directories()

    # Define data schema
    schema = define_schema()

    print("📡 Connecting to Kafka...")

    try:
        # Read streaming data from Kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "city-temperatures") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()

        print("✅ Connected to Kafka")
        print("🔍 Starting data analysis...")

        # Parse JSON messages from Kafka
        parsed_stream = kafka_stream \
            .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
            .select("data.*", "kafka_timestamp")

        # Convert timestamp and add processing time
        processed_stream = parsed_stream \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("processing_time", current_timestamp())

        # Apply anomaly detection logic
        with_anomalies = detect_temperature_anomalies(processed_stream)

        # Output stream 1: All data with anomaly flags - CSV
        all_data_query = with_anomalies \
            .select(
            "batch_id", "city", "temperature", "timestamp",
            "processing_time", "anomaly_type", "is_anomaly", "anomaly_details"
        ) \
            .writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "output/normal_data") \
            .option("checkpointLocation", "checkpoints/all_data") \
            .option("header", "true") \
            .trigger(processingTime=PROCESSING_INTERVAL) \
            .start()

        # Output stream 2: Anomalies only - CSV
        anomalies_only = with_anomalies.filter(col("is_anomaly") == True)

        anomalies_query = anomalies_only \
            .select(
            "batch_id", "city", "temperature", "timestamp",
            "processing_time", "anomaly_type", "anomaly_details"
        ) \
            .writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "output/anomalies") \
            .option("checkpointLocation", "checkpoints/anomalies") \
            .option("header", "true") \
            .trigger(processingTime=PROCESSING_INTERVAL) \
            .start()

        # Console output for real-time monitoring
        console_query = anomalies_only.select(
            "batch_id", "timestamp", "city", "temperature", "anomaly_type", "anomaly_details"
        ) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime=PROCESSING_INTERVAL) \
            .start()

        print("🎯 Application started! Monitoring anomalies...")
        print("📊 Data saved to CSV:")
        print("   • output/normal_data/ - all measurements + anomaly detection")
        print("   • output/anomalies/ - anomalies only")
        print("💻 Anomalies displayed on console")
        print(f"⏰ Time window: {WINDOW_DURATION}")
        print("\n🛑 Ctrl+C to stop")
        print("=" * 50)

        # Wait for streaming queries to finish
        all_data_query.awaitTermination()

    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure Kafka is running and Producer is sending data")

    except KeyboardInterrupt:
        print("\n⏹️  Stopping application...")
        # Stop all streaming queries gracefully
        all_data_query.stop()
        anomalies_query.stop()
        console_query.stop()
        print("✅ Application stopped")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()