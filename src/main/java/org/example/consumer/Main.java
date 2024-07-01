package org.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.Config;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    static int consumedCount = 0;
    static Object lock = new Object();
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        long startTime = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(Config.consumerThreadCount);
        List<Future<?>> futures = new ArrayList<>();

        int[][] counts = new int[Config.consumerThreadCount][Config.range + 1];

        for (int i = 0; i < Config.consumerThreadCount; i++) {
            int threadId = i;
            Runnable runnable = () -> {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.deserializer", StringDeserializer.class.getName());
                props.put("value.deserializer", IntegerDeserializer.class.getName());
                props.put("group.id", Integer.toString(threadId));

                int[] consumedNums = new int[1000_001];
                Consumer<String, Integer> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList("count"));

                while (true) {
                    ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100)); // Poll for records
                    for (ConsumerRecord<String, Integer> record : records) {
                        consumedNums[record.value()]++;
                    }
                    synchronized (lock) {
                        consumedCount += records.count();
                    }
                    if (consumedCount >= Config.messageCount / Config.nodeCount) {
                        break;
                    }
                    System.out.println(records.count());
                }
                consumer.close();
                counts[threadId] = consumedNums;

            };
            futures.add(executorService.submit(runnable));
        }

        for (var future : futures) {
            future.get();
        }
        executorService.shutdown();

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken(ms) : " + (endTime - startTime));

        Properties props = new Properties();
        props.put("bootstrap.servers", Config.mainNode + ":9092");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", IntegerSerializer.class.getName());

        Producer<Integer, Integer> producer = new KafkaProducer<>(props);
        for (int i = 1; i < 1000_001; i++) {
            int total = 0;
            for (int i1 = 0; i1 < Config.consumerThreadCount; i1++) {
                total += i1;
            }
            ProducerRecord<Integer, Integer> producerRecord = new ProducerRecord<>("aggregate", i, total);
            producer.send(producerRecord);
        }

    }
}