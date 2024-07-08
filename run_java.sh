#!/bin/bash

sh ~/Downloads/kafka/bin/kafka-server-stop.sh
sh ~/Downloads/kafka/bin/zookeeper-server-stop.sh
rm kafka.log zookeeper.log
rm -r /var/tmp/kafka-logs /tmp/zookeeper
sh ~/Downloads/kafka/bin/zookeeper-server-start.sh ~/Downloads/kafka/config/zookeeper.properties > zookeeper.log 2>&1 &
echo "Starting zookeeper"
sleep 5
sh ~/Downloads/kafka/bin/kafka-server-start.sh ~/Downloads/kafka/config/server.properties > kafka.log 2>&1 &
echo "Starting kafka"
sleep 5

sh ~/Downloads/kafka/bin/kafka-topics.sh --delete --topic count --bootstrap-server localhost:9092
sh ~/Downloads/kafka/bin/kafka-topics.sh --create --topic count --partitions 8 --bootstrap-server localhost:9092
sh ~/Downloads/kafka/bin/kafka-topics.sh --delete --topic aggregate --bootstrap-server localhost:9092
sh ~/Downloads/kafka/bin/kafka-topics.sh --create --topic aggregate --partitions 2 --bootstrap-server localhost:9092
#mvn exec:java -Dexec.mainClass="org.example.producer.Main"
#sh /home/ishad/Downloads/kafka/bin/kafka-server-stop.sh
#sh /home/ishad/Downloads/kafka/bin/zookeeper-server-stop.sh