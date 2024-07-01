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

public class Main {

    public static void main(String[] args) throws IOException {
        int[] counts = new int[Config.range + 1];
        int consumed = 0;
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.mainNode + ":9092");
        props.put("key.deserializer", IntegerDeserializer.class.getName());
        props.put("value.deserializer", IntegerDeserializer.class.getName());
        props.put("group.id", "aggregator");

        Consumer<Integer, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("aggregate"));

        while (true) {
            ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, Integer> record : records) {
                counts[record.key()] += record.value();
            }
            System.out.println(records.count());
            consumed += records.count();
            if (consumed >= Config.range * Config.nodeCount) {
                List<String> lines = new ArrayList<>();
                for (int i = 1; i < Config.range + 1; i++) {
                    lines.add(i + "," + counts[i]);
                }
                Files.write(Path.of("consumed.csv"), lines);
                break;
            }
        }
    }
}
