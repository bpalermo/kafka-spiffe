package io.github.bpalermo.kafka.spiffe.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder;
import io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory;
import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A client of one Kafka version against a broker of another (design, section 9, "Kafka"): the
 * broker runs in this process on the target's Kafka version, as in {@link BrokerTest}, and the
 * client in a JVM of its own ({@link ClientDriver}) whose class path is only the published jar, the
 * other version's {@code kafka-clients} and {@code slf4j-api}. The client produces and consumes
 * under ACLs on its {@code Spiffe:} principal, and keeps its connections while its trust domain
 * moves to a new root and its SVID is reissued by it; once the broker trusts only the new root, a
 * new connection that the same producer opens through its own factory succeeds, so that factory
 * presents the new SVID.
 *
 * <p>System properties: {@code client.kafka} and {@code client.version}, the client's {@code
 * kafka-clients} jar and its version; {@code jar.binary}, {@code jar.driver} and {@code jar.slf4j},
 * the rest of its class path.
 */
class CrossVersionTest {

  private static final String BROKER = "spiffe://example.org/broker";
  private static final String ADMIN = "spiffe://k8s.example.org/ns/ops/sa/admin";
  private static final String APP = "spiffe://k8s.example.org/ns/a/sa/b";
  private static final String TOPIC = "cross";
  private static final String BROKER_DOMAIN = "kafka.spiffe";
  private static final Duration WAIT = Duration.ofSeconds(60);

  private static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  private static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");
  private static final TestPki.Authority NEW_CLIENT_CA = TestPki.root("k8s.example.org");

  private final List<AutoCloseable> closeables = new ArrayList<>();
  private Path directory;
  private FakeWorkloadApi brokerAgent;
  private BrokerCluster cluster;

  @BeforeEach
  void cluster() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    directory = Files.createTempDirectory(Path.of("/tmp"), "cross");
    closeables.add(() -> Files.deleteIfExists(directory));
    brokerAgent = agent("broker.sock", BROKER, BROKER_CA, CLIENT_CA);

    Map<String, String> configs = new HashMap<>();
    configs.put(
        "listener.security.protocol.map", BrokerCluster.LISTENER + ":SSL,CONTROLLER:PLAINTEXT");
    configs.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    configs.put("ssl.client.auth", "required");
    configs.put("principal.builder.class", SpiffePrincipalBuilder.class.getName());
    configs.put("spiffe.endpoint.socket", "unix://" + brokerAgent.socket());
    configs.put("spiffe.startup.timeout.ms", "10000");
    configs.put("spiffe.peer.allowed.ids", BROKER);
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    configs.put("authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
    configs.put("super.users", "Spiffe:" + BROKER + ";Spiffe:" + ADMIN + ";User:ANONYMOUS");
    configs.put("offsets.topic.replication.factor", "1");
    configs.put("transaction.state.log.replication.factor", "1");
    configs.put("transaction.state.log.min.isr", "1");
    // So that the client's producer, idle, is made to open a new connection (ClientDriver).
    configs.put("connections.max.idle.ms", Long.toString(ClientDriver.IDLE.toMillis()));
    cluster = BrokerCluster.named();
    closeables.add(cluster::stop);
    cluster.start(1, configs);
  }

  @AfterEach
  void stop() throws Exception {
    for (int i = closeables.size() - 1; i >= 0; i--) {
      closeables.get(i).close();
    }
  }

  @Test
  void aClientOfAnotherVersionProducesAndConsumesAcrossARotationToANewRoot() throws Exception {
    // This JVM's Admin, of the broker's version, sets up the topic and the client's ACLs.
    FakeWorkloadApi adminAgent = agent("admin.sock", ADMIN, CLIENT_CA, BROKER_CA);
    try (Admin admin = Admin.create(clientConfigs(adminAgent))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
      admin
          .createAcls(
              List.of(
                  acl(ResourceType.TOPIC, TOPIC, AclOperation.WRITE),
                  acl(ResourceType.TOPIC, TOPIC, AclOperation.READ)))
          .all()
          .get();
      // DescribeAcls is answered by the broker, from the ACLs it has applied.
      eventually(
          () -> {
            try {
              return admin.describeAcls(AclBindingFilter.ANY).values().get().size() == 2;
            } catch (ExecutionException e) {
              return false;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return false;
            }
          },
          "the broker has the ACLs");
    }

    FakeWorkloadApi appAgent = agent("app.sock", APP, CLIENT_CA, BROKER_CA);
    List<String> classPath =
        List.of(
            property("jar.binary"),
            property("jar.driver"),
            property("client.kafka"),
            property("jar.slf4j"));
    Process driver =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                String.join(File.pathSeparator, classPath),
                ClientDriver.class.getName(),
                cluster.bootstrapServers(),
                appAgent.socket().toString(),
                BROKER,
                TOPIC)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start();
    closeables.add(driver::destroyForcibly);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    closeables.add(reader::shutdownNow);
    BufferedReader out =
        new BufferedReader(new InputStreamReader(driver.getInputStream(), StandardCharsets.UTF_8));

    assertEquals("version " + System.getProperty("client.version"), line(reader, out));
    String first = line(reader, out);
    assertTrue(first.startsWith("round "), first);

    // k8s.example.org moves to a new root: the broker trusts both, the client's SVID comes from
    // the new one, and the client takes it (its second update) before its next round.
    List<X509Certificate> both = List.of(CLIENT_CA.certificate(), NEW_CLIENT_CA.certificate());
    publishToBroker(both);
    appAgent.publish(
        new FakeWorkloadApi.Response()
            .svid(new FakeWorkloadApi.Svid(APP, svid(APP, NEW_CLIENT_CA), both, ""))
            .federated("spiffe://example.org", List.of(BROKER_CA.certificate())));
    OutputStream in = driver.getOutputStream();
    tell(in, "2");
    assertEquals(
        first, line(reader, out), "connection-creation-total of the producer and consumer");

    // The broker drops the old root: only the new SVID gets a new connection through, which the
    // same producer opens through the factory it has had from the start.
    publishToBroker(List.of(NEW_CLIENT_CA.certificate()));
    tell(in, "");
    String[] fresh = line(reader, out).split(" ");
    assertEquals("fresh", fresh[0]);
    assertTrue(
        Double.parseDouble(fresh[2]) > Double.parseDouble(fresh[1]),
        "the producer opened a new connection");
    assertTrue(driver.waitFor(WAIT.toSeconds(), TimeUnit.SECONDS), "the driver exits");
    assertEquals(0, driver.exitValue());
  }

  private static void tell(OutputStream in, String line) throws IOException {
    in.write((line + "\n").getBytes(StandardCharsets.UTF_8));
    in.flush();
  }

  /** Publishes the broker's SVID with {@code clientRoots} as k8s.example.org's bundle. */
  private void publishToBroker(List<X509Certificate> clientRoots) throws InterruptedException {
    long updates = updates();
    brokerAgent.publish(
        new FakeWorkloadApi.Response()
            .svid(FakeWorkloadApi.Svid.of(svid(BROKER, BROKER_CA), BROKER_CA))
            .federated("spiffe://k8s.example.org", clientRoots));
    eventually(() -> updates() > updates, "the broker's source takes the update");
  }

  /** The driver's next line of output, within {@link #WAIT}. */
  private static String line(ExecutorService reader, BufferedReader out) throws Exception {
    Future<String> line = reader.submit(out::readLine);
    return line.get(WAIT.toSeconds(), TimeUnit.SECONDS);
  }

  private static String property(String name) {
    return Path.of(System.getProperty(name)).toAbsolutePath().toString();
  }

  // --- the PKI ------------------------------------------------------------------------------

  /** An SVID valid now, with the other trust domain's bundle, as federated agents deliver it. */
  private static FakeWorkloadApi.Response response(
      String id, TestPki.Authority own, TestPki.Authority other) {
    String otherDomain = other == BROKER_CA ? "example.org" : "k8s.example.org";
    List<X509Certificate> bundle = List.of(other.certificate());
    return new FakeWorkloadApi.Response()
        .svid(FakeWorkloadApi.Svid.of(svid(id, own), own))
        .federated("spiffe://" + otherDomain, bundle);
  }

  /** An SVID valid now. */
  private static TestPki.Issued svid(String id, TestPki.Authority issuer) {
    Instant now = Instant.now();
    return issuer
        .leaf()
        .uris(id)
        .validity(now.minus(Duration.ofMinutes(5)), now.plus(Duration.ofHours(1)))
        .issue();
  }

  private FakeWorkloadApi agent(
      String name, String id, TestPki.Authority own, TestPki.Authority other) throws Exception {
    FakeWorkloadApi agent = FakeWorkloadApi.start(directory.resolve(name));
    closeables.add(() -> Files.deleteIfExists(agent.socket()));
    closeables.add(agent);
    agent.publish(response(id, own, other));
    return agent;
  }

  // --- the broker ---------------------------------------------------------------------------

  private Map<String, Object> clientConfigs(FakeWorkloadApi agent) {
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", cluster.bootstrapServers());
    configs.put("security.protocol", "SSL");
    configs.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    configs.put("spiffe.endpoint.socket", "unix://" + agent.socket());
    configs.put("spiffe.peer.allowed.ids", BROKER);
    configs.put("spiffe.startup.timeout.ms", "10000");
    configs.put("spiffe.metrics.jmx.domain", "kafka.spiffe.client");
    configs.put("default.api.timeout.ms", "30000");
    configs.put("request.timeout.ms", "20000");
    return configs;
  }

  private static AclBinding acl(ResourceType type, String name, AclOperation operation) {
    return new AclBinding(
        new ResourcePattern(type, name, PatternType.LITERAL),
        new AccessControlEntry("Spiffe:" + APP, "*", operation, AclPermissionType.ALLOW));
  }

  private long updates() {
    String name =
        BROKER_DOMAIN
            + ":type=x509-source,socket="
            + ObjectName.quote(brokerAgent.socket().toString());
    try {
      return ((Number)
              ManagementFactory.getPlatformMBeanServer()
                  .getAttribute(new ObjectName(name), "updates-total"))
          .longValue();
    } catch (JMException e) {
      throw new AssertionError("no updates-total of " + name, e);
    }
  }

  private static void eventually(BooleanSupplier condition, String what)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(WAIT);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(100);
    }
  }
}
