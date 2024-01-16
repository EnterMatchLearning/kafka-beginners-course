package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("CallToPrintStackTrace")
public class ProducerDemoKeys {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {
        logger.info("I am a kafka producer!");

        // create Producer Properties
        Properties properties = getProperties();

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String topic = "demo_java";
                String key = "id_" + j;
                String value = "hello world " + j;

                // create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        logger.info("Key: " + key + " | Partition: " + metadata.partition());
                    } else {
                        logger.error("Error while producing", e);
                    }
                });
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
