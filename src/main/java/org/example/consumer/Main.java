package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", IntegerDeserializer.class.getName());
        props.put("group.id", "my_group");

        Consumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("count"));

        long numberCount = (long) Math.pow(10, 9); // 1 billion
        long[] consumedNums = new long[1000000 + 1];

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < numberCount; i++) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100)); // Poll for records
            for (ConsumerRecord<String, Integer> record : records) {
                consumedNums[record.value()]++;
            }
        }
        consumer.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken(ms) : " + (endTime - startTime));

        Utils.writeToFile("consumedNums.txt", consumedNums);
    }
}