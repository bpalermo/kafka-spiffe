package io.github.bpalermo.kafka.spiffe.broker;

import java.util.Map;

/**
 * A KRaft cluster in this process, started with Kafka's own test kit, whose API differs between
 * 3.3, 3.9 and 4.x (design, section 9), so each line has its implementation. A test names the one
 * of its Kafka version in the system property {@value #PROPERTY}.
 *
 * <p>The brokers' listener {@value #LISTENER} is also the inter-broker one; its protocol and every
 * other setting come from the test. The controllers, of their own process roles, listen in
 * plaintext on {@code CONTROLLER}, which the 3.3 test kit does not let a test change.
 */
public interface KraftCluster {

  /** The system property naming the implementation, a class with a public no-argument one. */
  String PROPERTY = "kafka.spiffe.cluster";

  /** The brokers' listener. */
  String LISTENER = "EXTERNAL";

  /**
   * Formats and starts a cluster of one controller and {@code brokers} brokers with {@code configs}
   * on every node, and waits until the brokers are ready.
   */
  KraftCluster start(int brokers, Map<String, String> configs) throws Exception;

  /** The brokers' {@value #LISTENER} addresses, as {@code bootstrap.servers} takes them. */
  String bootstrapServers();

  /** Shuts the cluster down and deletes its directories. */
  void stop() throws Exception;

  /** The implementation named by {@value #PROPERTY}. */
  static KraftCluster named() throws ReflectiveOperationException {
    String name = System.getProperty(PROPERTY);
    if (name == null) {
      throw new IllegalStateException("No test cluster: set " + PROPERTY);
    }
    return (KraftCluster) Class.forName(name).getConstructor().newInstance();
  }
}
