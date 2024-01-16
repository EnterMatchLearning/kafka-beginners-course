package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        logger.info("I am a kafka consumer!");

        // create Producer Properties
        Properties properties = getProperties();

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        // send data
        producer.send(producerRecord);

        // tell the producer to flush data and block until it is done -- synchronous
        producer.flush();

        // close the producer
        producer.close();

    }

    private static Properties getProperties() {
        // kafka cluster properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "prompt-humpback-12136-us1-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cHJvbXB0LWh1bXBiYWNrLTEyMTM2JFVcf5SDIsh10XBCXkVZdjugnVXdqKL19uU\" password=\"ZGZiMDZmMjYtN2Q0ZS00MTM0LWJlZmMtMzg0YmNhMGZlZWFl\";");

        // producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
