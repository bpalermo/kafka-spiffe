package io.github.bpalermo.kafka.spiffe.broker;

import java.util.Map;
import kafka.testkit.BrokerNodes;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;

/** A {@link KraftCluster} on Kafka 3.3's test kit. */
public final class Kraft33 implements KraftCluster {

  private KafkaClusterTestKit cluster;

  @Override
  public KraftCluster start(int brokers, Map<String, String> configs) throws Exception {
    TestKitNodes.Builder nodes = new TestKitNodes.Builder().setNumControllerNodes(1);
    for (int id = 0; id < brokers; id++) {
      // The kit would replace the listener settings of configs; a broker's overrides win.
      nodes.addNode(BrokerNodes.withOverrides(id, configs));
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
