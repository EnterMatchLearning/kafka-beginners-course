package org.example;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

  public static RestHighLevelClient createOpenSearchClient() {
    String connString = "http://localhost:9200";
    //        String connString =
    // "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(
                      new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                  .setHttpClientConfigCallback(
                      httpAsyncClientBuilder ->
                          httpAsyncClientBuilder
                              .setDefaultCredentialsProvider(cp)
                              .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }

  private static KafkaConsumer<String, String> createKafkaConsumer() {
    String bootstrapServers = "localhost:19092";
    String groupId = "consumer-opensearch-demo";

    Properties properties = getProperties(groupId, bootstrapServers);

    return new KafkaConsumer<>(properties);
  }

  private static String extractId(String value) {
    return JsonParser.parseString(value)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }

  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    // first create an OpenSearch client
    RestHighLevelClient openSearchClient = createOpenSearchClient();

    // create our Kafka consumer
    KafkaConsumer<String, String> consumer = createKafkaConsumer();

    // Setup shutdown hook for graceful shutdown
    setupShutdownHook(logger, consumer);

    try (openSearchClient;
        consumer) {
      // we need to create the index in OpenSearch if it doesn't exist already
      boolean indexExists =
          openSearchClient
              .indices()
              .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

      if (!indexExists) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        logger.info("The Wikimedia index was created successfully.");
      } else {
        logger.info("The Wikimedia index already exists.");
      }

      // subscribe to the topic
      consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

      // poll for new data and insert into OpenSearch
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
        int recordCount = records.count();
        logger.info("Received " + recordCount + " record(s)");

        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> record : records) {
          // send the record into OpenSearch
          try {
            // extract the ID from the JSON value
            String id = extractId(record.value());
            IndexRequest indexRequest =
                new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
            //            IndexResponse response = openSearchClient.index(indexRequest,
            // RequestOptions.DEFAULT);
            bulkRequest.add(indexRequest);
            //             logger.info(response.getId());
          } catch (Exception e) {
          }
        }

        if (bulkRequest.numberOfActions() > 0) {
          BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          logger.info("Inserted " + bulkResponse.getItems().length + " record(s)");

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          // commit the offsets after the batch is processed
          consumer.commitSync();
          logger.info("Offsets have been committed!");
        }
      }

    } catch (WakeupException e) {
      logger.info("Consumer is starting to shut down");
    } catch (Exception e) {
      logger.error("Unexpected exception in the consumer", e);
    } finally {
      consumer.close();
      openSearchClient.close();
      logger.info("The consumer is now gracefully shut down");
    }
  }

  private static Properties getProperties(String groupId, String bootstrapServers) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return properties;
  }

  private static void setupShutdownHook(Logger logger, KafkaConsumer<String, String> consumer) {
    // get a reference to the main thread
    Thread mainThread = Thread.currentThread();

    // adding the shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                  consumer.wakeup();

                  // join the main thread to allow the execution of the code in the main thread
                  try {
                    mainThread.join();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }));
  }
}
