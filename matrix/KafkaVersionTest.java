package io.github.bpalermo.kafka.spiffe.matrix;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Proves a Kafka version's repository is the one on the run time class path: the version recorded
 * in the kafka-clients jar is the one the target was declared for. No broker is started here;
 * broker tests are in {@code src/test/java/.../broker}, one {@code kafka_version_test} per version.
 */
class KafkaVersionTest {
  @Test
  void runsOnTheDeclaredKafkaClients() throws IOException {
    String expected = System.getProperty("kafka.spiffe.expected.kafka.version");
    assertNotNull(expected, "the target must set kafka.spiffe.expected.kafka.version");
    // AppInfoParser, which reads this file, moved in 4.x; the file did not.
    Properties properties = new Properties();
    try (InputStream in =
        KafkaVersionTest.class
            .getClassLoader()
            .getResourceAsStream("kafka/kafka-version.properties")) {
      assertNotNull(in, "kafka-clients is not on the class path");
      properties.load(in);
    }
    assertEquals(expected, properties.getProperty("version"));
  }
}
