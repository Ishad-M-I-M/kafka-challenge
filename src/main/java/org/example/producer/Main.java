package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.Utils;

import java.util.Properties;
import java.util.Random;

// Time taken to produce: 420569ms
public class Main {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", IntegerSerializer.class.getName());

        Producer<String, Integer> producer = new KafkaProducer<>(props);
        Random random = new Random();

        long numberCount = (long) Math.pow(10, 9); // 1 billion
        long[] producedNums = new long[1000000 + 1];

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < numberCount; i++) {
            int num = random.nextInt(1, 1000000 + 1);
            ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("count", "number", num);
            producer.send(producerRecord);
            producedNums[num] = producedNums[num] + 1;
        }
        producer.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken(ms) : " + (endTime - startTime));

        Utils.writeToFile("producedNums.txt", producedNums);
    }
}