package io.github.bpalermo.kafka.spiffe.broker;

import java.util.Map;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;

/** A {@link KraftCluster} on Kafka 3.9's test kit. */
public final class Kraft39 implements KraftCluster {

  private KafkaClusterTestKit cluster;

  @Override
  public KraftCluster start(int brokers, Map<String, String> configs) throws Exception {
    TestKitNodes nodes =
        new TestKitNodes.Builder()
            .setCombined(false)
            .setNumControllerNodes(1)
            .setNumBrokerNodes(brokers)
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
