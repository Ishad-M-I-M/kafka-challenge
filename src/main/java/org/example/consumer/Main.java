package org.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
//    static int consumedCount = 0;
    static AtomicInteger consumedCount = new AtomicInteger(0);
//    static Object lock = new Object();

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        long startTime = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(Config.consumerThreadCount);
        List<Future<?>> futures = new ArrayList<>();

        int[][] counts = new int[Config.consumerThreadCount][Config.range + 1];

        for (int i = 0; i < Config.consumerThreadCount; i++) {
            int threadId = i;
            Runnable runnable = () -> {
                Properties props = new Properties();
                props.put("bootstrap.servers", Config.myNode + ":9092");
                props.put("key.deserializer", IntegerDeserializer.class.getName());
                props.put("value.deserializer", IntegerDeserializer.class.getName());
                props.put("group.id", "my-group");
                props.put("enable.auto.commit", "false");
                props.put("auto.offset.reset", "earliest");

                int[] consumedNums = new int[1000_001];
                Consumer<String, Integer> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList("count"));

                while (true) {
                    ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100)); // Poll for records
                    if (consumedCount.get() >= Config.messageCount) {
                        break;
                    }
                    for (ConsumerRecord<String, Integer> record : records) {
                        consumedNums[record.value()]++;
                        consumedCount.incrementAndGet();
                    }
                    consumer.commitAsync();

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

        // Ensure that the producer properties are set correctly
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.mainNode + ":9092");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", IntegerSerializer.class.getName());
        props.put("batch.size", 32768); // Increase batch size for better throughput
        props.put("linger.ms", 10);

        Producer<Integer, Integer> producer = new KafkaProducer<>(props);
        List<String> lines = new ArrayList<>();
        for (int i = 1; i < 1000_001; i++) {
            int total = 0;
            for (int i1 = 0; i1 < Config.consumerThreadCount; i1++) {
                total += counts[i1][i];
            }
            ProducerRecord<Integer, Integer> producerRecord = new ProducerRecord<>("aggregate", i, total);
            int finalI = i;
            int finalTotal = total;
//            producer.send(producerRecord, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e!= null) {
//                        System.out.println(e.getMessage());
//                    } else {
//                        lines.add(finalI + "," + finalTotal);
//                    }
//                }
//            });

            producer.send(producerRecord);
        }
        producer.close();
        //Files.write(Path.of("sent.csv"), lines);
    }
}
