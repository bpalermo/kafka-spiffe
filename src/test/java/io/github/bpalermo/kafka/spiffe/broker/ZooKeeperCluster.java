package io.github.bpalermo.kafka.spiffe.broker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

/**
 * A {@link BrokerCluster} of ZooKeeper brokers, on the test utilities of Kafka 3.0 to 3.9: {@code
 * EmbeddedZookeeper} and {@code TestUtils.createServer}, whose signatures are the same on both.
 */
public final class ZooKeeperCluster implements BrokerCluster {

  private final List<KafkaServer> servers = new ArrayList<>();
  private final List<Path> directories = new ArrayList<>();
  private EmbeddedZookeeper zookeeper;

  @Override
  public BrokerCluster start(int brokers, Map<String, String> configs) throws Exception {
    zookeeper = new EmbeddedZookeeper();
    for (int id = 0; id < brokers; id++) {
      Path logs = Files.createTempDirectory("zookeeper-broker-" + id);
      directories.add(logs);
      Properties properties = new Properties();
      properties.putAll(configs);
      properties.put("broker.id", Integer.toString(id));
      properties.put("zookeeper.connect", "127.0.0.1:" + zookeeper.port());
      // Port 0: the broker registers the port it bound.
      properties.put("listeners", LISTENER + "://127.0.0.1:0");
      properties.put("inter.broker.listener.name", LISTENER);
      properties.put("log.dirs", logs.toString());
      properties.put("controlled.shutdown.enable", "false");
      // Started, and registered in ZooKeeper, when it returns.
      servers.add(TestUtils.createServer(KafkaConfig.fromProps(properties), Time.SYSTEM));
    }
    return this;
  }

  @Override
  public boolean kraft() {
    return false;
  }

  @Override
  public String bootstrapServers() {
    ListenerName listener = ListenerName.normalised(LISTENER);
    return servers.stream()
        .map(server -> "127.0.0.1:" + server.boundPort(listener))
        .collect(Collectors.joining(","));
  }

  @Override
  public void stop() throws Exception {
    // Every step runs whatever an earlier one threw; the first failure is thrown at the end.
    List<Exception> failures = new ArrayList<>();
    for (KafkaServer server : servers) {
      attempt(
          failures,
          () -> {
            server.shutdown();
            server.awaitShutdown();
          });
    }
    if (zookeeper != null) {
      attempt(failures, zookeeper::shutdown);
    }
    for (Path directory : directories) {
      attempt(failures, () -> Utils.delete(directory.toFile()));
    }
    if (!failures.isEmpty()) {
      Exception first = failures.get(0);
      failures.stream().skip(1).forEach(first::addSuppressed);
      throw first;
    }
  }

  /** One step of {@link #stop}. */
  private interface Step {
    void run() throws Exception;
  }

  private static void attempt(List<Exception> failures, Step step) {
    try {
      step.run();
    } catch (Exception e) {
      failures.add(e);
    }
  }
}
