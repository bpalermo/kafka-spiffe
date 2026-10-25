package io.github.bpalermo.kafka.spiffe.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * End to end (design, section 9): an {@code apache/kafka} broker with only the published jar added,
 * configured through the image's environment as docs/operator.md says, and a client in a container
 * of its own, each identified by its Docker labels to the agent of its own SPIRE trust domain, the
 * two trust domains federated. SVIDs live 60 seconds, so the run sees several rotations on both
 * sides, and each agent is replaced while records flow.
 *
 * <p>The image is {@code -Dkafka.image}; the jar {@code -Djar.binary}.
 */
class KafkaE2eTest {

  private static final Logger LOG = Logger.getLogger(KafkaE2eTest.class.getName());

  private static final String BROKER_DOMAIN = "example.org";
  private static final String CLIENT_DOMAIN = "k8s.example.org";
  private static final String BROKER_ID = "spiffe://example.org/kafka/broker";
  private static final Duration SVID_TTL = Duration.ofSeconds(60);

  /** One record a second, so the producer's run is this many seconds. */
  private static final int RECORDS = 120;

  private static final String SOCKET = "unix:///run/spire/agent/sockets/agent.sock";
  private static final String JAR = "/opt/kafka-spiffe/kafka-spiffe.jar";
  private static final String BIN = "/opt/kafka/bin/";

  /**
   * The rule of docs/operator.md's Compose example, as the YAML single-quoted string hands it to
   * the image: every backslash doubled, since the image writes it into server.properties as it is.
   */
  private static final String MAPPING_RULES =
      "RULE:^spiffe:\\\\/\\\\/k8s\\\\.example\\\\.org\\\\/ns\\\\/payments\\\\/sa\\\\/.*$"
          + "/spiffe-group:k8s.example.org:payments/, DEFAULT";

  /** The principal the rule gives the client; the ACLs name it, not the client's ID. */
  private static final String CLIENT_PRINCIPAL = "Spiffe:spiffe-group:k8s.example.org:payments";

  /** A client of the broker, in the broker's container or the client's. */
  private static final String CLIENT_PROPERTIES =
      """
      security.protocol=SSL
      ssl.engine.factory.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory
      spiffe.endpoint.socket=%s
      spiffe.peer.allowed.ids=%s
      """
          .formatted(SOCKET, BROKER_ID);

  private static final String CLIENT_ID = "spiffe://k8s.example.org/ns/payments/sa/api";

  /**
   * The producer's logging: Kafka's tools log at WARN, and the library says when it takes a new
   * SVID at INFO. Kafka 3.9's tools read the first (log4j), 4.x's the second (log4j 2); each
   * ignores the other's property.
   */
  private static final String TOOLS_LOG4J =
      """
      log4j.rootLogger=WARN, stderr
      log4j.appender.stderr=org.apache.log4j.ConsoleAppender
      log4j.appender.stderr.Target=System.err
      log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
      log4j.appender.stderr.layout.ConversionPattern=[%d] %p %m (%c)%n
      log4j.logger.io.github.bpalermo.kafka.spiffe=INFO
      """;

  private static final String TOOLS_LOG4J2 =
      """
      appender.stderr.type=Console
      appender.stderr.name=STDERR
      appender.stderr.target=SYSTEM_ERR
      appender.stderr.layout.type=PatternLayout
      appender.stderr.layout.pattern=[%d] %p %m (%c)%n
      rootLogger.level=WARN
      rootLogger.appenderRef.stderr.ref=STDERR
      logger.spiffe.name=io.github.bpalermo.kafka.spiffe
      logger.spiffe.level=INFO
      """;

  private static final Pattern NEW_SVID =
      Pattern.compile("New X\\.509 SVID (spiffe://\\S+) from the Workload API");

  private Network network;
  private SpireDomain brokerDomain;
  private SpireDomain clientDomain;
  private GenericContainer<?> broker;
  private GenericContainer<?> client;

  @BeforeEach
  void startSpire() throws Exception {
    network = Network.newNetwork();
    brokerDomain = new SpireDomain(network, BROKER_DOMAIN, CLIENT_DOMAIN);
    clientDomain = new SpireDomain(network, CLIENT_DOMAIN, BROKER_DOMAIN);
    brokerDomain.startServer();
    clientDomain.startServer();
    brokerDomain.trust(CLIENT_DOMAIN, clientDomain.bundle());
    clientDomain.trust(BROKER_DOMAIN, brokerDomain.bundle());
    brokerDomain.register("/kafka/broker", "app:kafka-broker", CLIENT_DOMAIN, SVID_TTL);
    clientDomain.register("/ns/payments/sa/api", "app:payments-api", BROKER_DOMAIN, SVID_TTL);
    brokerDomain.startAgent();
    clientDomain.startAgent();
  }

  @AfterEach
  void stop() {
    for (AutoCloseable closeable :
        new AutoCloseable[] {client, broker, clientDomain, brokerDomain}) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (Exception e) {
          LOG.warning(() -> "Cannot stop " + closeable + ": " + e);
        }
      }
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  void recordsFlowAcrossRotationsAndAgentRestarts() throws Exception {
    DockerImageName image = DockerImageName.parse(System.getProperty("kafka.image"));
    MountableFile jar = MountableFile.forHostPath(Path.of(System.getProperty("jar.binary")), 0644);

    broker = broker(image, jar);
    broker.start();
    client =
        workload(image, jar, clientDomain, "app", "payments-api")
            .withCommand("sleep", "infinity")
            .withNetworkAliases("client");
    client.start();
    for (GenericContainer<?> container : new GenericContainer<?>[] {broker, client}) {
      container.copyFileToContainer(
          MountableFile.forHostPath(write(CLIENT_PROPERTIES), 0644), "/tmp/client.properties");
    }
    client.copyFileToContainer(
        MountableFile.forHostPath(write(TOOLS_LOG4J), 0644), "/tmp/log4j.properties");
    client.copyFileToContainer(
        MountableFile.forHostPath(write(TOOLS_LOG4J2), 0644), "/tmp/log4j2.properties");

    // As the broker's own identity, a super user: the topic and the client's ACLs.
    admin("kafka-topics.sh", "--create", "--topic", "orders", "--partitions", "1");
    admin(
        "kafka-acls.sh",
        "--add",
        "--allow-principal",
        CLIENT_PRINCIPAL,
        "--operation",
        "Write",
        "--operation",
        "Read",
        "--operation",
        "Describe",
        "--topic",
        "orders");
    admin(
        "kafka-acls.sh",
        "--add",
        "--allow-principal",
        CLIENT_PRINCIPAL,
        "--operation",
        "Read",
        "--group",
        "payments");

    // One record a second on one producer, while both agents are replaced in turn.
    Instant started = Instant.now();
    SpireDomain.exec(
        client,
        "sh",
        "-c",
        ("export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:/tmp/log4j.properties"
                + " -Dlog4j2.configurationFile=/tmp/log4j2.properties\";"
                + " i=0; while [ $i -lt %d ]; do echo order-$i; i=$((i+1)); sleep 1; done"
                + " | %skafka-console-producer.sh --bootstrap-server broker:9093"
                + " --producer.config /tmp/client.properties --topic orders"
                + " --producer-property acks=all > /tmp/producer.log 2>&1;"
                + " echo $? > /tmp/producer.exit")
            .formatted(RECORDS, BIN)
            .transform(script -> "nohup sh -c '" + script + "' > /dev/null 2>&1 &"));
    sleepUntil(started.plus(Duration.ofSeconds(RECORDS / 3)));
    long brokerSvidsBefore = svids(broker.getLogs(), BROKER_ID);
    LOG.info("Replacing the broker's agent");
    brokerDomain.restartAgent();
    sleepUntil(started.plus(Duration.ofSeconds(2 * RECORDS / 3)));
    long producerSvidsBefore =
        svids(client.execInContainer("cat", "/tmp/producer.log").getStdout(), CLIENT_ID);
    LOG.info("Replacing the client's agent");
    clientDomain.restartAgent();

    String exit = waitForFile(client, "/tmp/producer.exit", Duration.ofSeconds(RECORDS));
    String producerLog = client.execInContainer("cat", "/tmp/producer.log").getStdout();
    assertEquals("0", exit.strip(), producerLog);

    // A new consumer, after the rotations and both restarts, reads everything back.
    Container.ExecResult consumed =
        client.execInContainer(
            BIN + "kafka-console-consumer.sh",
            "--bootstrap-server",
            "broker:9093",
            "--consumer.config",
            "/tmp/client.properties",
            "--topic",
            "orders",
            "--group",
            "payments",
            "--from-beginning",
            "--max-messages",
            String.valueOf(RECORDS),
            "--timeout-ms",
            "60000");
    // 4.x also prints a deprecation notice for --consumer.config, which 3.9 needs.
    Set<String> records =
        new TreeSet<>(consumed.getStdout().lines().filter(l -> l.startsWith("order-")).toList());
    Set<String> expected = new TreeSet<>();
    for (int i = 0; i < RECORDS; i++) {
      expected.add("order-" + i);
    }
    assertEquals(expected, records, consumed.getStderr() + producerLog);

    // Both took a new SVID several times, at about half of each 60-second lifetime, and each
    // from its new agent: the stream recovered in the broker and in the producer's own JVM, which
    // the consumer's handshakes alone would not show.
    long brokerSvids = svids(broker.getLogs(), BROKER_ID);
    assertTrue(brokerSvids >= 4, "the broker took " + brokerSvids + " SVIDs");
    assertTrue(
        brokerSvids > brokerSvidsBefore,
        "the broker took no SVID after its agent was replaced (" + brokerSvids + " in all)");
    long producerSvids = svids(producerLog, CLIENT_ID);
    assertTrue(producerSvids >= 3, "the producer took " + producerSvids + " SVIDs\n" + producerLog);
    assertTrue(
        producerSvids > producerSvidsBefore,
        "the producer took no SVID after its agent was replaced\n" + producerLog);
  }

  /** How many SVIDs {@code log} says were taken; every one must be {@code id}. */
  private static long svids(String log, String id) {
    List<String> ids =
        log.lines().map(NEW_SVID::matcher).filter(Matcher::find).map(m -> m.group(1)).toList();
    assertTrue(ids.stream().allMatch(id::equals), "SVIDs other than " + id + ": " + ids);
    return ids.size();
  }

  /** The broker, configured only through the image's environment (docs/operator.md). */
  private GenericContainer<?> broker(DockerImageName image, MountableFile jar) {
    return workload(image, jar, brokerDomain, "app", "kafka-broker")
        .withNetworkAliases("broker")
        .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .withEnv("KAFKA_NODE_ID", "1")
        .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
        .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
        .withEnv("KAFKA_LISTENERS", "MTLS://0.0.0.0:9093,CONTROLLER://localhost:9094")
        .withEnv("KAFKA_ADVERTISED_LISTENERS", "MTLS://broker:9093")
        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "MTLS:SSL,CONTROLLER:PLAINTEXT")
        .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "MTLS")
        .withEnv("KAFKA_SSL_CLIENT_AUTH", "required")
        .withEnv(
            "KAFKA_SSL_ENGINE_FACTORY_CLASS",
            "io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory")
        .withEnv(
            "KAFKA_PRINCIPAL_BUILDER_CLASS",
            "io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder")
        .withEnv("KAFKA_SPIFFE_ENDPOINT_SOCKET", SOCKET)
        .withEnv("KAFKA_SPIFFE_PEER_ALLOWED_TRUST_DOMAINS", BROKER_DOMAIN + "," + CLIENT_DOMAIN)
        .withEnv("KAFKA_SPIFFE_PRINCIPAL_MAPPING_RULES", MAPPING_RULES)
        .withEnv(
            "KAFKA_AUTHORIZER_CLASS_NAME",
            "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
        .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
        .withEnv("KAFKA_SUPER_USERS", "Spiffe:" + BROKER_ID + ";User:ANONYMOUS")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .waitingFor(
            Wait.forLogMessage(".*Kafka Server started.*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
  }

  /**
   * A container of the image, with the jar on the class path and the socket of {@code domain}'s
   * agent, which knows it by its label.
   */
  private GenericContainer<?> workload(
      DockerImageName image, MountableFile jar, SpireDomain domain, String label, String value) {
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withLabel(label, value)
        .withEnv("CLASSPATH", JAR)
        .withCopyToContainer(jar, JAR)
        .withFileSystemBind(
            domain.socketDirectory().toString(), "/run/spire/agent/sockets", BindMode.READ_ONLY)
        .withLogConsumer(
            frame -> LOG.info(() -> "[" + value + "] " + frame.getUtf8String().stripTrailing()));
  }

  private void admin(String tool, String... args) throws Exception {
    String[] command = new String[args.length + 5];
    command[0] = BIN + tool;
    command[1] = "--bootstrap-server";
    command[2] = "broker:9093";
    command[3] = "--command-config";
    command[4] = "/tmp/client.properties";
    System.arraycopy(args, 0, command, 5, args.length);
    LOG.info(SpireDomain.exec(broker, command));
  }

  private static String waitForFile(GenericContainer<?> container, String path, Duration timeout)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      Container.ExecResult result = container.execInContainer("cat", path);
      if (result.getExitCode() == 0 && !result.getStdout().isBlank()) {
        return result.getStdout();
      }
      Thread.sleep(1000);
    }
    throw new AssertionError(path + " did not appear within " + timeout);
  }

  private static void sleepUntil(Instant instant) throws InterruptedException {
    long millis = Duration.between(Instant.now(), instant).toMillis();
    if (millis > 0) {
      Thread.sleep(millis);
    }
  }

  private static Path write(String content) {
    try {
      Path file = Files.createTempFile("client", ".properties");
      file.toFile().deleteOnExit();
      Files.writeString(file, content);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
