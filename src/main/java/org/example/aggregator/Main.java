package org.example.aggregator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.example.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        AtomicInteger consumed = new AtomicInteger(0);
        int[][] finalcounts = new int[Config.aggregateReceiveThreadCount][Config.range+1];

        ExecutorService executorService = Executors.newFixedThreadPool(Config.aggregateReceiveThreadCount);
        List<Future<?>> futures = new ArrayList<>();
        for (int j=0; j < Config.aggregateReceiveThreadCount; j++) {
            int finalJ = j;
            Runnable runnable = () -> {
                int[] counts = new int[Config.range + 1];

                Properties props = new Properties();
                props.put("bootstrap.servers", Config.mainNode + ":9092");
                props.put("key.deserializer", IntegerDeserializer.class.getName());
                props.put("value.deserializer", IntegerDeserializer.class.getName());
                props.put("group.id", "aggregator");
                props.put("enable.auto.commit", "false");
                props.put("auto.offset.reset", "latest");

                Consumer<Integer, Integer> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Collections.singletonList("aggregate"));

                while (true) {
                    ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Integer, Integer> record : records) {
                        counts[record.key()] += record.value();
                    }
                    System.out.println(records.count());
                    consumer.commitSync();
                    consumed.addAndGet(records.count());
                    if (consumed.get() >= Config.range * Config.nodeCount) {
                        break;
                    }
                }
                finalcounts[finalJ] = counts;
            };
            futures.add(executorService.submit(runnable));
        }

        for (var future : futures) {
            future.get();
        }

        executorService.shutdown();

        ArrayList<String> lines = new ArrayList<>();
        int eachCount;
        for (int k=1; k < Config.range + 1; k++) {
            eachCount = 0;
            for (int k1 = 0; k1 < Config.aggregateReceiveThreadCount; k1++) {
                eachCount += finalcounts[k1][k];
            }
            lines.add(k + "," + eachCount);
        }
        Files.write(Path.of("aggregated.csv"), lines);
    }
}
