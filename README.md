# Launching with locally installed dependecies

1. Launching zookeeper

```bash
cd ~/kafka/kafka_2.13-3.9.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Launching Kafka Server

```bash
cd ~/kafka/kafka_2.13-3.9.1
bin/kafka-server-start.sh config/server.properties
```
3. Creating Topic (if not existing)

```bash
bin/kafka-topics.sh --create --topic city-temperatures --bootstrap-server localhost:9092
```

4. Launch Producer.py
5. Launch spark_detection.py

# Launching on AWS LernerLab (for the first time)

1. Start Lab
2. Create instance of EC2. Choose Ubuntu as operating system and choose key pair.
3. Wait for the instance to turn on.
4. Log in to the EC2 for example, via local console:
```bash
ssh -i Key.pem ubuntu@IP_ADDRESS
```
where _Key.pem_ is path to the key pair and IP_ADDRESS is IP adress of the EC2.
5. Update system and install docker.io and docker-compose. Then adds the user to the docker group and exit EC2.
```bash
sudo apt update
sudo apt install docker.io -y
sudo apt  install docker-compose
sudo usermod -aG docker ubuntu
exit
```
6. Log in again to the EC2 machine
7. Clone repository of the project and build docker.
```bash
git clone https://github.com/Bebo457/ASEED-Project.git
cd ASEED-Project/
docker-compose build
```
8. Start the system
```bash
./start.sh
```

# Launching on AWS LernerLab
1. Start Lab
2. Log in to the EC2 for example, via local console:
```bash
ssh -i Key.pem ubuntu@IP_ADDRESS
```
where _Key.pem_ is path to the key pair and IP_ADDRESS is IP adress of the EC2.
3. Go in to the docker directory and start system.
```bash
cd ASEED-Project/
./start.sh
```

# Using system
## Output
output files are saved in the _output_ directory. In the _normal_data_ directory there are csv files of all data captured by spark detections system and in the _anomalies_ directory
there are only detected anomalies data.
## Commands
* Start system 
```bash
./start.sh
```
* Stop system
```bash
./stop.sh 
```
* See logs of the subsystem
```bash
docker-compose logs -f temperature-producer
docker-compose logs -f spark-detector
docker-compose logs -f kafka
```


# Requirements
kafka-python: 2.2.15
pyspark: 3.5.6
pandas: 2.3.0
numpy: 2.3.1
pyarrow: 21.0.0

# Setup for local installation

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
