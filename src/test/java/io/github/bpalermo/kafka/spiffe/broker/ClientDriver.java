package io.github.bpalermo.kafka.spiffe.broker;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Run by {@link CrossVersionTest} in a JVM of its own, whose class path holds the published jar,
 * another Kafka version's {@code kafka-clients} and this driver: a producer and a consumer through
 * the engine factory, against the test's broker.
 *
 * <p>It prints {@code version <kafka-clients version>}, then {@code round <connections>} after
 * producing five records and consuming them back; waits for a line on its standard input; does
 * another round on the same clients; and prints {@code fresh} once a new producer, on a new
 * connection, has had a record acknowledged. {@code <connections>} is the producer's and the
 * consumer's {@code connection-creation-total}. A failure is an exception, which exits 1.
 *
 * <p>Arguments: the bootstrap servers, the Workload API socket, the broker's SPIFFE ID, the topic.
 */
public final class ClientDriver {

  private static final Duration WAIT = Duration.ofSeconds(60);

  private ClientDriver() {}

  public static void main(String[] args) throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", args[0]);
    configs.put("security.protocol", "SSL");
    configs.put(
        "ssl.engine.factory.class", "io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory");
    configs.put("spiffe.endpoint.socket", "unix://" + args[1]);
    configs.put("spiffe.peer.allowed.ids", args[2]);
    configs.put("spiffe.startup.timeout.ms", "10000");
    configs.put("request.timeout.ms", "20000");
    configs.put("max.block.ms", "30000");
    configs.put("acks", "all");
    configs.put("auto.offset.reset", "earliest");
    String topic = args[3];
    // System.out flushes each line, which the test reads as it comes.
    try (BufferedReader in =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      run(configs, topic, in, System.out);
    }
  }

  private static void run(
      Map<String, Object> configs, String topic, BufferedReader in, PrintStream out)
      throws Exception {
    out.println("version " + version());
    try (KafkaProducer<String, String> producer =
            new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());
        KafkaConsumer<String, String> consumer =
            new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer())) {
      consumer.assign(List.of(new TopicPartition(topic, 0)));
      round(producer, consumer, topic);
      out.println("round " + (connections(producer.metrics()) + connections(consumer.metrics())));
      if (in.readLine() == null) {
        throw new IllegalStateException("standard input closed");
      }
      round(producer, consumer, topic);
      out.println("round " + (connections(producer.metrics()) + connections(consumer.metrics())));
    }
    try (KafkaProducer<String, String> fresh =
        new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer())) {
      fresh.send(new ProducerRecord<>(topic, 0, "fresh", "fresh")).get();
    }
    out.println("fresh");
  }

  /** Five records produced, and read back by the consumer. */
  private static void round(
      KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer, String topic)
      throws Exception {
    String marker = Long.toString(System.nanoTime());
    for (int i = 0; i < 5; i++) {
      producer.send(new ProducerRecord<>(topic, 0, marker + i, marker)).get();
    }
    int read = 0;
    Instant deadline = Instant.now().plus(WAIT);
    while (read < 5) {
      if (Instant.now().isAfter(deadline)) {
        throw new IllegalStateException("read " + read + " of 5 records of round " + marker);
      }
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
        if (marker.equals(record.value())) {
          read++;
        }
      }
    }
  }

  private static double connections(Map<MetricName, ? extends Metric> metrics) {
    double total = 0;
    for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
      MetricName name = metric.getKey();
      if (name.name().equals("connection-creation-total")
          && (name.group().equals("producer-metrics") || name.group().equals("consumer-metrics"))) {
        total += ((Number) metric.getValue().metricValue()).doubleValue();
      }
    }
    return total;
  }

  /** The version of the kafka-clients on the class path, as it records it. */
  private static String version() throws Exception {
    Properties properties = new Properties();
    try (InputStream in =
        ClientDriver.class.getResourceAsStream("/kafka/kafka-version.properties")) {
      properties.load(in);
    }
    return properties.getProperty("version");
  }
}
