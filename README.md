# Launching

1. Launching zookeeper

```
cd ~/kafka/kafka_2.13-3.9.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Launching Kafka Server

```
cd ~/kafka/kafka_2.13-3.9.1
bin/kafka-server-start.sh config/server.properties
```
3. Creating Topic (if not existing)

```
bin/kafka-topics.sh --create --topic city-temperatures --bootstrap-server localhost:9092
```

4. Launch Producer.py
5. Launch spark_detection.py
# Requirements
* kafka 2.13-3.9.1
* spark 4.0.0
* Java 17+ (OpenJDK recommended)
* Python 3.8+

## Setup

### Unpack downloaded packages to directories:
```bash
mkdir ~/kafka ~/spark
cd ~/kafka && tar -xzf kafka_2.13-3.9.1.tgz
cd ~/spark && tar -xzf spark-4.0.0-bin-hadoop3.tgz
```
### Set environment variables:
```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
echo 'export SPARK_HOME=~/spark/spark-4.0.0-bin-hadoop3' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
```
### Install Python dependencies:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
