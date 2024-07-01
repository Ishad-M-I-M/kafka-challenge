package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.Config;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/*
* Time taken:
* Threads - Time(ms)
* 1 - 5964
* 2 - 4307
* 4 - 3454
* 8 - 3408
* 16 - 3539
* */
public class Main {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(Config.producerThreadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < Config.producerThreadCount; i++) {
            Runnable runnable = () -> {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer", StringSerializer.class.getName());
                props.put("value.serializer", IntegerSerializer.class.getName());

                Producer<String, Integer> producer = new KafkaProducer<>(props);
                Random random = new Random();

                for (int i1 = 0; i1 < Config.messageCount/Config.producerThreadCount; i1++) {
                    int num = random.nextInt(1, Config.range + 1);
                    ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("count", null, num);
                    producer.send(producerRecord);
                }

                producer.close();
            };
            futures.add(executorService.submit(runnable));
        }

        for (Future future: futures) {
            future.get();
        }
        executorService.shutdown();

        long endTime = System.currentTimeMillis();
        System.out.println("Time taken(ms) : " + (endTime - startTime));
    }
}