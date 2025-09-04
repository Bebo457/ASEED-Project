FROM eclipse-temurin:17-jre-focal

# Instalacja Python i narzędzi systemowych
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Ustawienie katalogu roboczego
WORKDIR /app

# Kopiowanie plików projektu
COPY requirements.txt .
COPY Producer.py .
COPY spark_detection.py .

# Instalacja zależności Python z Hadoop 3
ENV PYSPARK_HADOOP_VERSION=3
RUN pip3 install --no-cache-dir -r requirements.txt -v

# Tworzenie katalogów wyjściowych
RUN mkdir -p output/normal_data output/anomalies output/aggregations checkpoints

# Port dla Spark UI
EXPOSE 4040

# Domyślna komenda
CMD ["python3", "Producer.py"]