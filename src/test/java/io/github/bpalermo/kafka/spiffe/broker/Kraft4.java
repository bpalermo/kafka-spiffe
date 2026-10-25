package io.github.bpalermo.kafka.spiffe.broker;

import java.util.Map;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

/** A {@link BrokerCluster} on the test kit of Kafka 4.x. */
public final class Kraft4 implements BrokerCluster {

  private KafkaClusterTestKit cluster;

  @Override
  public BrokerCluster start(
      int brokers, Map<String, String> configs, Map<Integer, Map<String, String>> overrides)
      throws Exception {
    TestKitNodes nodes =
        new TestKitNodes.Builder()
            .setCombined(false)
            .setNumControllerNodes(1)
            .setNumBrokerNodes(brokers)
            // Keyed by node ID; the brokers' are 0, 1, ...
            .setPerServerProperties(overrides)
            .build();
    KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
    configs.forEach(builder::setConfigProp);
    cluster = builder.build();
    cluster.format();
    cluster.startup();
    cluster.waitForReadyBrokers();
    return this;
  }

  @Override
  public boolean kraft() {
    return true;
  }

  @Override
  public String bootstrapServers() {
    return cluster.bootstrapServers();
  }

  @Override
  public void stop() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }
}
