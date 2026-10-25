package io.github.bpalermo.kafka.spiffe.jar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The published jar on a class path of its own (design, section 9, "The jar"): {@link JarDriver}
 * runs in a new JVM whose class path is the jar, the driver, and either only {@code kafka-clients}
 * and {@code slf4j-api} (property {@code jar.kafka} and {@code jar.slf4j}) or a broker
 * distribution's whole {@code libs/} (property {@code jar.libs}, one jar of that directory). The
 * Workload APIs run here, in the test's JVM, with SVIDs valid now: the driver uses the system
 * clock.
 */
class JarClassPathTest {

  private static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  private static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private Path directory;
  private FakeWorkloadApi brokerAgent;
  private FakeWorkloadApi clientAgent;

  @BeforeEach
  void agents() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    directory = Files.createTempDirectory(Path.of("/tmp"), "jar");
    brokerAgent = FakeWorkloadApi.start(directory.resolve("broker.sock"));
    clientAgent = FakeWorkloadApi.start(directory.resolve("client.sock"));
    brokerAgent.publish(response(BROKER_CA, "spiffe://example.org/broker", CLIENT_CA));
    clientAgent.publish(response(CLIENT_CA, "spiffe://k8s.example.org/ns/a/sa/b", BROKER_CA));
  }

  @AfterEach
  void stop() throws Exception {
    if (brokerAgent != null) {
      brokerAgent.close();
      clientAgent.close();
      Files.deleteIfExists(directory);
    }
  }

  /** An SVID valid now, with the other trust domain's bundle, as federated agents deliver it. */
  private static FakeWorkloadApi.Response response(
      TestPki.Authority own, String id, TestPki.Authority other) {
    Instant now = Instant.now();
    TestPki.Issued svid =
        own.leaf()
            .uris(id)
            .validity(now.minus(Duration.ofMinutes(5)), now.plus(Duration.ofHours(1)))
            .issue();
    String otherDomain = other == BROKER_CA ? "example.org" : "k8s.example.org";
    return new FakeWorkloadApi.Response()
        .svid(FakeWorkloadApi.Svid.of(svid, own))
        .federated("spiffe://" + otherDomain, List.of(other.certificate()));
  }

  private static Path property(String name) {
    return Path.of(System.getProperty(name)).toAbsolutePath();
  }

  /** The class path besides the jar and the driver: Kafka's, as the property names it. */
  private static List<Path> kafka() throws Exception {
    String libs = System.getProperty("jar.libs");
    if (libs == null) {
      return List.of(property("jar.kafka"), property("jar.slf4j"));
    }
    try (Stream<Path> jars = Files.list(property("jar.libs").getParent())) {
      List<Path> all = jars.filter(p -> p.toString().endsWith(".jar")).sorted().toList();
      assertTrue(all.size() > 50, "a broker's libs/ holds about a hundred jars: " + all);
      return all;
    }
  }

  @Test
  void theJarHandshakesAndBuildsPrincipalsOnKafkasClassPathAlone() throws Exception {
    List<String> classPath = new ArrayList<>();
    classPath.add(property("jar.binary").toString());
    classPath.add(property("jar.driver").toString());
    classPath.add(property("jar.handshake").toString());
    kafka().forEach(p -> classPath.add(p.toString()));

    Path output = directory.resolve("driver.log");
    Process driver =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                String.join(File.pathSeparator, classPath),
                "io.github.bpalermo.kafka.spiffe.jar.JarDriver",
                property("jar.binary").toString(),
                clientAgent.socket().toString(),
                brokerAgent.socket().toString())
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      boolean exited = driver.waitFor(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      String log = Files.readString(output, StandardCharsets.UTF_8);
      assertTrue(exited, "the driver did not finish in " + TIMEOUT + ":\n" + log);
      assertEquals(0, driver.exitValue(), log);
    } finally {
      driver.destroyForcibly();
      Files.deleteIfExists(output);
    }
  }
}
