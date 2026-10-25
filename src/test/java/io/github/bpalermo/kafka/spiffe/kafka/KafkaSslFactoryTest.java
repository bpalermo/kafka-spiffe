package io.github.bpalermo.kafka.spiffe.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * The factory inside Kafka's own SslFactory (kafka-clients 3.0.0), as an inter-broker listener
 * configures it: Kafka instantiates it by class, and validates it by a handshake with itself. It
 * runs on the system clock and the process's shared sources, as in a broker.
 */
class KafkaSslFactoryTest {

  private static final TestPki.Authority CA = TestPki.root("example.org");

  private static Map<String, Object> configs(Path socket, String allowedIds) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SpiffeSslEngineFactory.class);
    configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
    configs.put("ssl.client.auth", "required");
    configs.put("spiffe.endpoint.socket", "unix://" + socket);
    configs.put("spiffe.peer.allowed.ids", allowedIds);
    configs.put("spiffe.metrics.jmx.enabled", "false");
    configs.put("spiffe.startup.timeout.ms", "10000");
    return configs;
  }

  @Test
  void kafkasValidationHandshakePassesWithTheBrokersOwnIdAllowed() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    Path directory = Files.createTempDirectory(Path.of("/tmp"), "ksf");
    Instant now = Instant.now();
    TestPki.Issued broker =
        CA.leaf()
            .uris("spiffe://example.org/broker")
            .validity(now.minus(Duration.ofMinutes(5)), now.plus(Duration.ofHours(1)))
            .issue();
    try (FakeWorkloadApi agent = FakeWorkloadApi.start(directory.resolve("agent.sock"))) {
      agent.publish(new FakeWorkloadApi.Response().svid(FakeWorkloadApi.Svid.of(broker, CA)));

      SslFactory server = new SslFactory(Mode.SERVER, null, true);
      server.configure(configs(agent.socket(), "spiffe://example.org/broker"));
      assertNotNull(server.createSslEngine("10.0.0.1", 9093));
      SslFactory client = new SslFactory(Mode.CLIENT, null, true);
      client.configure(configs(agent.socket(), "spiffe://example.org/broker"));
      assertNotNull(client.createSslEngine("10.0.0.1", 9093));
      server.close();
      client.close();

      SslFactory refusing = new SslFactory(Mode.SERVER, null, true);
      ConfigException e =
          assertThrows(
              ConfigException.class,
              () ->
                  refusing.configure(configs(agent.socket(), "spiffe://example.org/someone-else")));
      assertNotNull(e.getMessage());
      List.of(server, client, refusing).forEach(SslFactory::close);
    } finally {
      Files.deleteIfExists(directory);
    }
  }
}
