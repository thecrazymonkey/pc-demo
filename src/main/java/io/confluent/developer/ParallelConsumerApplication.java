package io.confluent.developer;


import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import io.confluent.parallelconsumer.PCRetriableException;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.Value;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor;


/**
 * Simple "hello world" Confluent Parallel Consumer application that simply consumes records from Kafka and writes the
 * message values to a file.
 */
public class ParallelConsumerApplication {
  public static final String METRICS_ENDPOINT = "/prometheus";
  public static final String VALIDATION_ENDPOINT = "/user";

  private static final Logger LOGGER = LoggerFactory.getLogger(ParallelConsumerApplication.class.getName());
  private final ParallelStreamProcessor<String, Object> parallelConsumer;
  private final ExecutorService metricsEndpointExecutor;

  private final PrometheusMeterRegistry meterRegistry;
  private final String url;
  OkHttpClient client = new OkHttpClient.Builder()
          .readTimeout(30000, TimeUnit.MILLISECONDS)
          .writeTimeout(30000, TimeUnit.MILLISECONDS)
          .connectionPool(new ConnectionPool(100, 10000, TimeUnit.MILLISECONDS))
          .build();
  void setupServiceEndpoint() {
    try {
      final var server = HttpServer.create(new InetSocketAddress(7001), 0);
      server.setExecutor(new ThreadPoolExecutor(20, 20, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100)));
      server.createContext(METRICS_ENDPOINT, httpExchange -> {
        String response = meterRegistry.scrape();
        httpExchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = httpExchange.getResponseBody()) {
          os.write(response.getBytes());
        }
      });
      server.createContext(VALIDATION_ENDPOINT, httpExchange -> {
        String userId = httpExchange.getRequestURI().getPath().split("/")[2];
        LOGGER.info("Request: {}, Thread: {}", userId,Thread.currentThread().getId());
        String response = "valid";
        int toSleep = 500;
        if (userId != null && userId.equals("222")) {
          toSleep = 12000;
        }
        try {
          Thread.sleep(toSleep);
        } catch (InterruptedException e) {
          LOGGER.error("Sleep interrupted");
        }
        httpExchange.sendResponseHeaders(200, response.length());
        try (OutputStream os = httpExchange.getResponseBody()) {
          os.write(response.getBytes());
        }
      });
      metricsEndpointExecutor.submit(server::start);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Application that runs a given Confluent Parallel Consumer, calling the given handler method per record.
   *
   * @param parallelConsumer the Confluent Parallel Consumer instance
   * @param metricsEndpointExecutor the executor service for the metrics endpoint
  // * @param meterRegistry the Prometheus meter registry
   * @param url to send external requests to
   */
  public ParallelConsumerApplication(final ParallelStreamProcessor<String, Object> parallelConsumer,
                                     final ExecutorService metricsEndpointExecutor,
                                      final PrometheusMeterRegistry meterRegistry,
                                     String url) {
    this.parallelConsumer = parallelConsumer;
    this.metricsEndpointExecutor = metricsEndpointExecutor;
     this.meterRegistry = meterRegistry;
    this.url = url;
  }

  /**
   * Close the parallel consumer on application shutdown
   */
  public void shutdown() {
    LOGGER.info("shutting down");
    if (parallelConsumer != null) {
      parallelConsumer.close();
      metricsEndpointExecutor.shutdownNow();
    }
  }
  private Result processBrokerRecord(ConsumerRecord<String, Object> consumerRecord) throws IOException {
    // add http client code for sending http get request to localhost at port 8080
    Request request = new Request.Builder()
            .url(url+"/"+consumerRecord.key())
            .build();
    LOGGER.info("Request: {} with value {}, for user {}", request.url(), consumerRecord.value(),consumerRecord.key());

    Response response = client.newCall(request).execute();
    // add http client code for sending http get request to localhost at port 8080
    String responseString = response.body() != null ? response.body().string() : null;
    LOGGER.info("Response code: {}, for user {}, result {}",response.code(), consumerRecord.key(),responseString);
    return new Result(responseString);
  }

  @Value
  static class Result {
    String payload;
  }

  /**
   * Subscribes to the configured input topic and calls (blocking) `poll` method.
   *
   * @param appProperties application and consumer properties
   */
  public void runConsume(final Properties appProperties) {
    String topic = appProperties.getProperty("input.topic.name");
    String outputTopic = appProperties.getProperty("output.topic.name");

    LOGGER.info("Subscribing Parallel Consumer to consume from {} topic", topic);
    parallelConsumer.subscribe(Collections.singletonList(topic));

    LOGGER.info("Polling {} for records. This method blocks", topic);
    parallelConsumer.pollAndProduceMany(context -> {
              var consumerRecord = context.getSingleConsumerRecord();
              Result result;
              LOGGER.info("Message received for user {}", consumerRecord.key());
              LOGGER.info("Message type received {}", consumerRecord.value().getClass());
              try {
                result = processBrokerRecord(consumerRecord);
              } catch (IOException e) {
                LOGGER.info("Retrying ");
                throw new PCRetriableException();
              }
              ArrayList<ProducerRecord<String,Object>> msgList = new ArrayList<>();
              if (consumerRecord.value() instanceof io.confluent.developer.Recorddifferent) {
                LOGGER.info("Recorddifferent received for user {}", consumerRecord.key());
                msgList.add(new ProducerRecord<>("recorddifferent", consumerRecord.key(), consumerRecord.value()));
                // send and additional record to the broker - different type + different topic
                io.confluent.developer.Recordnew myRecorddnew = new io.confluent.developer.Recordnew()
                        .withSource(((Recorddifferent) consumerRecord.value()).getSourceApplication())
                        .withTarget(((Recorddifferent) consumerRecord.value()).getTargetApplication())
                        .withTransaction(((Recorddifferent) consumerRecord.value()).getTransactionID());
                msgList.add(new ProducerRecord<>("recordnew", myRecorddnew));

              } else if (consumerRecord.value() instanceof io.confluent.developer.Record) {
                LOGGER.info("Record received for user {}", consumerRecord.key());
                msgList.add(new ProducerRecord<>("record", consumerRecord.key(), consumerRecord.value()));
                // send and additional record to the broker - different type + different topic
                io.confluent.developer.Recordnew myRecorddnew = new io.confluent.developer.Recordnew()
                        .withSource(((Record) consumerRecord.value()).getSourceApplication())
                        .withTarget(((Record) consumerRecord.value()).getTargetApplication())
                        .withTransaction(((Record) consumerRecord.value()).getTranID());
                msgList.add(new ProducerRecord<>("recordnew", myRecorddnew));
              }
              return msgList;
            }, consumeProduceResult -> LOGGER.info("Message {} saved to broker at offset {}",
                    consumeProduceResult.getOut(),
                    consumeProduceResult.getMeta().offset())
    );
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "This program takes one argument: the path to an environment configuration file.");
    }
    ExecutorService metricsEndpointExecutor = Executors.newSingleThreadExecutor();
    PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
//    JmxMeterRegistry meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
    final Properties appProperties = PropertiesUtil.loadProperties(args[0]);
    KafkaClientMetrics kafkaClientMetrics;
    // random consumer group ID for rerun convenience
    String groupId = "parallel-consumer-app-group"; // + nextInt();
    appProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    // construct parallel consumer
    appProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
    appProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
    appProperties.put(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY, "javaTypeName");
    appProperties.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    appProperties.put(KafkaJsonSchemaSerializerConfig.LATEST_CACHE_TTL, 100);
    // appProperties.put(KafkaJsonSchemaSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, "io.confluent.kafka.serializers.subject.TopicNameStrategy");
    appProperties.put(KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, true);
    appProperties.put(KafkaJsonSchemaSerializerConfig.LATEST_COMPATIBILITY_STRICT, false);
    appProperties.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
    final Consumer<String, Object> consumer = new KafkaConsumer<>(appProperties);
    Producer<String, Object> producer = new KafkaProducer<>(appProperties);
    final String instanceId = UUID.randomUUID().toString();
    final ParallelConsumerOptions<String, Object> options = ParallelConsumerOptions.<String, Object>builder()
        .ordering(ParallelConsumerOptions.ProcessingOrder.valueOf(appProperties.getProperty("processing_order","KEY")))
        .maxConcurrency(Integer.parseInt(appProperties.getProperty("max_concurrency", String.valueOf(ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY))))
        .consumer(consumer)
        .producer(producer)
        .meterRegistry(meterRegistry)
        .metricsTags(Tags.of(Tag.of("application-type", "pc-rest-caller"),Tag.of("application-team", "pc-rest-caller-team")))
        .pcInstanceTag(instanceId)
        .thresholdForTimeSpendInQueueWarning(Duration.ofMillis(10000))
        .commitMode(ParallelConsumerOptions.CommitMode.valueOf(appProperties.getProperty("commit_mode","PERIODIC_CONSUMER_ASYNCHRONOUS")))
        .build();
    ParallelStreamProcessor<String, Object> eosStreamProcessor = createEosStreamProcessor(options);
    kafkaClientMetrics = new KafkaClientMetrics(consumer); //<4>
    kafkaClientMetrics.bindTo(meterRegistry);
    // run the consumer!
    final ParallelConsumerApplication consumerApplication = new ParallelConsumerApplication(eosStreamProcessor,
            metricsEndpointExecutor,
             meterRegistry,
            appProperties.getProperty("url","http://localhost:8080/user"));
    consumerApplication.setupServiceEndpoint();
    Runtime.getRuntime().addShutdownHook(new Thread(consumerApplication::shutdown));
    // produce sample records
    io.confluent.developer.Recorddifferent myRecorddifferent = new io.confluent.developer.Recorddifferent()
            .withSourceApplication("source-app")
            .withTargetApplication("target-app")
            .withMessageId(12345.0)
            .withTransactionID("tranid");
    io.confluent.developer.Record myRecord = new io.confluent.developer.Record()
            .withTraceID("traceid")
            .withTranID("tranid")
            .withCallerContenxtID("contextid")
            .withTargetApplication("target-app")
            .withSourceApplication("source-app")
            .withMessageType("test");
    producer.send(new ProducerRecord<>("composite", "test-recorddifferent", myRecorddifferent),
            ((metadata, exception) -> {
              if (exception != null) {
                System.err.printf("Producing %s resulted in error %s", myRecorddifferent, exception);
              }
            }));
    producer.send(new ProducerRecord<>("composite", "test-record",
                   myRecord),
            ((metadata, exception) -> {
              if (exception != null) {
                System.err.printf("Producing %s resulted in error %s", myRecord, exception);
              }
      }));
//    introduce validation error on the broker side
//    io.confluent.developer.RecordnewTest myRecordnew = new io.confluent.developer.RecordnewTest()
//            .withAmount("12345.0")
//            .withCost("12345.0")
//            .withSource("source-app")
//            .withTarget("target-app")
//            .withTransaction("tranid");

//    while (true) {
//        producer.send(new ProducerRecord<>("recordtest", "test-record", myRecordnew),
//                ((metadata, exception) -> {
//                    if (exception != null) {
//                        System.err.printf("Producing %s resulted in error %s", myRecord, exception);
//                    }
//                }));
//        Thread.sleep(10000);
//    }
//    producer.send(new ProducerRecord<>("recorddifferent", "test-recorddifferent", myRecorddifferent),
//            ((metadata, exception) -> {
//              if (exception != null) {
//                System.err.printf("Producing %s resulted in error %s", myRecorddifferent, exception);
//              }
//            }));
    consumerApplication.runConsume(appProperties);
  }

}

