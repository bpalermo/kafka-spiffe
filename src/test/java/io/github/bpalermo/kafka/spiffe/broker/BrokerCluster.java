package io.github.bpalermo.kafka.spiffe.broker;

import java.util.Map;

/**
 * A cluster in this process: KRaft, started with Kafka's own test kit, whose API differs between
 * 3.3, 3.9 and 4.x, or ZooKeeper, started with the test utilities of 3.0 to 3.9 (design, section
 * 9), so each line and mode has its implementation. A test names the one of its Kafka version in
 * the system property {@value #PROPERTY}.
 *
 * <p>The brokers' listener {@value #LISTENER} is also the inter-broker one; its protocol and every
 * other setting come from the test. A KRaft cluster's controllers, of their own process roles,
 * listen in plaintext on {@code CONTROLLER}, which the 3.3 test kit does not let a test change.
 */
public interface BrokerCluster {

  /** The system property naming the implementation, a class with a public no-argument one. */
  String PROPERTY = "kafka.spiffe.cluster";

  /** The brokers' listener. */
  String LISTENER = "EXTERNAL";

  /**
   * Formats and starts a cluster of {@code brokers} brokers, and the controller or ZooKeeper they
   * need, with {@code configs} on every node, and waits until the brokers are ready.
   */
  BrokerCluster start(int brokers, Map<String, String> configs) throws Exception;

  /** Whether the cluster is KRaft, where requests are forwarded to a controller, or ZooKeeper. */
  boolean kraft();

  /** The brokers' {@value #LISTENER} addresses, as {@code bootstrap.servers} takes them. */
  String bootstrapServers();

  /** Shuts the cluster down and deletes its directories. */
  void stop() throws Exception;

  /** The implementation named by {@value #PROPERTY}. */
  static BrokerCluster named() throws ReflectiveOperationException {
    String name = System.getProperty(PROPERTY);
    if (name == null) {
      throw new IllegalStateException("No test cluster: set " + PROPERTY);
    }
    return (BrokerCluster) Class.forName(name).getConstructor().newInstance();
  }
}
