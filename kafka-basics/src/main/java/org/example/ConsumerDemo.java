package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        logger.info("I am a kafka consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer Properties
        Properties properties = getProperties(groupId);

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(List.of(topic));

        // poll for data
        while (true) {
            logger.info("Polling for data...");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                logger.info("Key: " + record.key() + " | Value: " + record.value());
                logger.info("Partition: " + record.partition() + " | Offset: " + record.offset());
            });
        }

    }

    private static Properties getProperties(String groupId) {
        // kafka cluster properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "prompt-humpback-12136-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvbXB0LWh1bXBiYWNrLTEyMTM2JFVcf5SDIsh10XBCXkVZdjugnVXdqKL19uU\" password=\"ZGZiMDZmMjYtN2Q0ZS00MTM0LWJlZmMtMzg0YmNhMGZlZWFl\";");

        // consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
