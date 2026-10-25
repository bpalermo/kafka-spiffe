package io.github.bpalermo.kafka.spiffe.broker;

import java.util.HashMap;
import java.util.Map;
import kafka.testkit.BrokerNodes;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;

/** A {@link BrokerCluster} on Kafka 3.3's test kit. */
public final class Kraft33 implements BrokerCluster {

  private KafkaClusterTestKit cluster;

  @Override
  public BrokerCluster start(
      int brokers, Map<String, String> configs, Map<Integer, Map<String, String>> overrides)
      throws Exception {
    TestKitNodes.Builder nodes = new TestKitNodes.Builder().setNumControllerNodes(1);
    for (int id = 0; id < brokers; id++) {
      // The kit would replace the listener settings of configs; a broker's overrides win.
      Map<String, String> broker = new HashMap<>(configs);
      broker.putAll(overrides.getOrDefault(id, Map.of()));
      nodes.addNode(BrokerNodes.withOverrides(id, broker));
    }
    KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes.build());
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
    return cluster.clientProperties().getProperty("bootstrap.servers");
  }

  @Override
  public void stop() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
  }
}
