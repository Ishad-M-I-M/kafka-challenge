#!/bin/bash

sh ~/Downloads/kafka_2.13-3.7.0/bin/kafka-server-stop.sh
sh ~/Downloads/kafka_2.13-3.7.0/bin/zookeeper-server-stop.sh
rm kafka.log zookeeper.log
rm -r /var/tmp/kafka-logs /tmp/zookeeper
sh ~/Downloads/kafka_2.13-3.7.0/bin/zookeeper-server-start.sh ~/Downloads/kafka_2.13-3.7.0/config/zookeeper.properties > zookeeper.log 2>&1 &
echo "Starting zookeeper"
sleep 5
sh ~/Downloads/kafka_2.13-3.7.0/bin/kafka-server-start.sh ~/Downloads/kafka_2.13-3.7.0/config/server.properties > kafka.log 2>&1 &
echo "Starting kafka"
sleep 5

sh ~/Downloads/kafka_2.13-3.7.0/bin/kafka-topics.sh --delete --topic counts --bootstrap-server localhost:9092
sh ~/Downloads/kafka_2.13-3.7.0/bin/kafka-topics.sh --create --topic counts --partitions 8 --bootstrap-server localhost:9092
#mvn exec:java -Dexec.mainClass="org.example.producer.Main"
#sh /home/ishad/Downloads/kafka_2.13-3.7.0/bin/kafka-server-stop.sh
#sh /home/ishad/Downloads/kafka_2.13-3.7.0/bin/zookeeper-server-stop.sh