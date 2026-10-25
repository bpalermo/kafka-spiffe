package io.github.bpalermo.kafka.spiffe.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder;
import io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory;
import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A cluster halfway through the move from spiffe-helper that the operator's guide describes
 * (docs/operator.md, "Moving from spiffe-helper"), on the Kafka version and mode of the target:
 * broker 0 is switched to the library, broker 1 still loads its SVID and the trust bundles as PEM
 * files, as spiffe-helper writes them, through Kafka's own engine factory and principal builder.
 * The ACLs and {@code super.users} hold both names of each workload, as the guide's second step has
 * them: the DN principal ({@code User:CN=...}) the old brokers give and the {@code Spiffe:}
 * principal the switched ones give.
 *
 * <p>The guide's claims, checked: the two kinds of broker replicate to each other; a client still
 * on files is admitted by the switched broker through its new ACL and by the old one through its
 * old ACL, and is refused by the switched broker without the new one; a switched client is admitted
 * by both.
 */
class MigrationTest {

  private static final String BROKER = "spiffe://example.org/broker";
  private static final String ADMIN = "spiffe://k8s.example.org/ns/ops/sa/admin";
  private static final String APP = "spiffe://k8s.example.org/ns/a/sa/b";

  /** A workload whose ACLs nobody moved over: only its DN principal has any. */
  private static final String LEGACY = "spiffe://k8s.example.org/ns/legacy/sa/l";

  private static final Duration WAIT = Duration.ofSeconds(60);

  private static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  private static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");

  private static Path directory;
  private static FakeWorkloadApi brokerAgent;
  private static BrokerCluster cluster;

  private final List<AutoCloseable> closeables = new ArrayList<>();
  private int agents;

  @BeforeAll
  static void cluster() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    directory = Files.createTempDirectory(Path.of("/tmp"), "migration");
    brokerAgent = FakeWorkloadApi.start(directory.resolve("broker.sock"));
    brokerAgent.publish(response(BROKER, BROKER_CA, CLIENT_CA));

    // What every broker has: a TLS listener, the authorizer, both names of each super user.
    Map<String, String> configs = new HashMap<>();
    configs.put(
        "listener.security.protocol.map", BrokerCluster.LISTENER + ":SSL,CONTROLLER:PLAINTEXT");
    configs.put("ssl.client.auth", "required");
    cluster = BrokerCluster.named();
    String superUsers = "Spiffe:" + BROKER + ";Spiffe:" + ADMIN;
    superUsers += ";User:" + dn(BROKER) + ";User:" + dn(ADMIN);
    if (cluster.kraft()) {
      configs.put(
          "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
      configs.put("super.users", superUsers + ";User:ANONYMOUS");
    } else {
      configs.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
      configs.put("super.users", superUsers);
    }
    configs.put("offsets.topic.replication.factor", "2");
    configs.put("transaction.state.log.replication.factor", "2");
    configs.put("transaction.state.log.min.isr", "1");

    // Broker 0, switched: the library, its keys, no keystore or truststore.
    Map<String, String> switched = new HashMap<>();
    switched.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    switched.put("principal.builder.class", SpiffePrincipalBuilder.class.getName());
    switched.put("spiffe.endpoint.socket", "unix://" + brokerAgent.socket());
    switched.put("spiffe.startup.timeout.ms", "10000");
    switched.put("spiffe.peer.allowed.ids", BROKER);
    switched.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");

    // Broker 1, not yet: Kafka's defaults on the files, a truststore of both trust domains, and
    // no host name check, which an SVID with only a URI name would fail.
    Map<String, String> files = files(BROKER);
    files.put("ssl.truststore.type", "PEM");
    files.put("ssl.truststore.certificates", pem(BROKER_CA.certificate(), CLIENT_CA.certificate()));
    files.put("ssl.endpoint.identification.algorithm", "");

    cluster.start(2, configs, Map.of(0, switched, 1, files));
  }

  @AfterAll
  static void stopCluster() throws Exception {
    if (cluster != null) {
      cluster.stop();
    }
    if (brokerAgent != null) {
      brokerAgent.close();
      Files.deleteIfExists(directory.resolve("broker.sock"));
      Files.deleteIfExists(directory);
    }
  }

  @AfterEach
  void close() throws Exception {
    for (int i = closeables.size() - 1; i >= 0; i--) {
      closeables.get(i).close();
    }
  }

  @Test
  void aSwitchedBrokerAndAnOldOneReplicateToEachOther() throws Exception {
    Admin admin = admin(newClient(ADMIN));
    NewTopic topic =
        new NewTopic("replicated", Map.of(0, List.of(0, 1), 1, List.of(1, 0)))
            .configs(Map.of("min.insync.replicas", "2"));
    admin.createTopics(List.of(topic)).all().get();
    awaitIsr(admin, "replicated");
    KafkaProducer<String, String> producer = producer(newClient(ADMIN));
    // acks=all with min.insync.replicas=2: each write waits for the other broker's fetch, the
    // switched broker's from the old one and the old one's from the switched one.
    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<>("replicated", i % 2, "k" + i, "v" + i)).get();
    }
    awaitIsr(admin, "replicated");
  }

  @Test
  void clientsOfBothKindsAreAdmittedByBothNamesAndRefusedWithoutTheNewOne() throws Exception {
    Admin admin = admin(newClient(ADMIN));
    createTopic(admin, "orders");
    admin
        .createAcls(
            List.of(
                acl("User:" + dn(APP), AclOperation.WRITE),
                acl("Spiffe:" + APP, AclOperation.WRITE),
                acl("User:" + dn(LEGACY), AclOperation.WRITE)))
        .all()
        .get();

    // Partition 0 is led by the switched broker, partition 1 by the old one.
    KafkaProducer<String, String> old = producer(oldClient(APP));
    KafkaProducer<String, String> switched = producer(newClient(APP));
    for (int partition = 0; partition < 2; partition++) {
      send(old, new ProducerRecord<>("orders", partition, "old", "v"));
      send(switched, new ProducerRecord<>("orders", partition, "switched", "v"));
    }

    // Both brokers have the batch of ACLs by now. The switched broker names LEGACY Spiffe:,
    // which no ACL covers, so it refuses it whichever broker answers its metadata request.
    KafkaProducer<String, String> legacy = producer(oldClient(LEGACY));
    ProducerRecord<String, String> record = new ProducerRecord<>("orders", 0, "legacy", "v");
    ExecutionException refused =
        assertThrows(ExecutionException.class, () -> legacy.send(record).get());
    assertInstanceOf(AuthorizationException.class, refused.getCause());
  }

  // --- the PKI ------------------------------------------------------------------------------

  /** The subject of each workload's SVID, which Kafka's default builder makes the principal. */
  private static String dn(String id) {
    return "CN=" + id.substring(id.lastIndexOf('/') + 1) + ",O=example";
  }

  /** An SVID valid now, with that DN, from {@code own}. */
  private static TestPki.Issued svid(String id, TestPki.Authority own) {
    Instant now = Instant.now();
    return own.leaf()
        .uris(id)
        .dn(dn(id))
        .validity(now.minus(Duration.ofMinutes(5)), now.plusSeconds(3600))
        .issue();
  }

  /** {@code id}'s SVID and the other trust domain's bundle, as federated agents deliver them. */
  private static FakeWorkloadApi.Response response(
      String id, TestPki.Authority own, TestPki.Authority other) {
    String otherDomain = other == BROKER_CA ? "example.org" : "k8s.example.org";
    return new FakeWorkloadApi.Response()
        .svid(FakeWorkloadApi.Svid.of(svid(id, own), own))
        .federated("spiffe://" + otherDomain, List.of(other.certificate()));
  }

  /**
   * Kafka's own engine factory and principal builder on a PEM keystore of {@code id}'s SVID: what
   * spiffe-helper's files give a broker or a client that is not switched yet.
   */
  private static Map<String, String> files(String id) throws CertificateEncodingException {
    TestPki.Issued svid = svid(id, id.startsWith("spiffe://example.org/") ? BROKER_CA : CLIENT_CA);
    Map<String, String> configs = new HashMap<>();
    configs.put(
        "ssl.engine.factory.class", "org.apache.kafka.common.security.ssl.DefaultSslEngineFactory");
    configs.put(
        "principal.builder.class",
        "org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder");
    configs.put("ssl.keystore.type", "PEM");
    configs.put(
        "ssl.keystore.certificate.chain", pem(svid.chain().toArray(X509Certificate[]::new)));
    configs.put("ssl.keystore.key", pem("PRIVATE KEY", svid.key().getEncoded()));
    return configs;
  }

  private static String pem(X509Certificate... certificates) throws CertificateEncodingException {
    StringBuilder pem = new StringBuilder();
    for (X509Certificate certificate : certificates) {
      pem.append(pem("CERTIFICATE", certificate.getEncoded()));
    }
    return pem.toString();
  }

  private static String pem(String type, byte[] der) {
    Base64.Encoder base64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
    return "-----BEGIN "
        + type
        + "-----\n"
        + base64.encodeToString(der)
        + "\n-----END "
        + type
        + "-----\n";
  }

  // --- clients ------------------------------------------------------------------------------

  /** A switched client of {@code id}, on a Workload API of its own. */
  private Map<String, Object> newClient(String id) throws Exception {
    FakeWorkloadApi agent =
        FakeWorkloadApi.start(directory.resolve("client-" + agents++ + ".sock"));
    closeables.add(agent);
    closeables.add(() -> Files.deleteIfExists(agent.socket()));
    agent.publish(response(id, CLIENT_CA, BROKER_CA));
    Map<String, Object> configs = common();
    configs.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    configs.put("spiffe.endpoint.socket", "unix://" + agent.socket());
    configs.put("spiffe.peer.allowed.ids", BROKER);
    configs.put("spiffe.startup.timeout.ms", "10000");
    configs.put("spiffe.metrics.jmx.domain", "kafka.spiffe.client");
    return configs;
  }

  /** A client of {@code id} still on files: its SVID, and a truststore of the brokers' domain. */
  private static Map<String, Object> oldClient(String id) throws Exception {
    Map<String, Object> configs = common();
    files(id).forEach(configs::put);
    configs.remove("principal.builder.class");
    configs.put("ssl.truststore.type", "PEM");
    configs.put("ssl.truststore.certificates", pem(BROKER_CA.certificate()));
    configs.put("ssl.endpoint.identification.algorithm", "");
    return configs;
  }

  private static Map<String, Object> common() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", cluster.bootstrapServers());
    configs.put("security.protocol", "SSL");
    configs.put("default.api.timeout.ms", "30000");
    configs.put("request.timeout.ms", "20000");
    configs.put("max.block.ms", "30000");
    return configs;
  }

  private Admin admin(Map<String, Object> configs) {
    Admin admin = Admin.create(configs);
    closeables.add(admin);
    return admin;
  }

  private KafkaProducer<String, String> producer(Map<String, Object> configs) {
    configs.put("acks", "all");
    KafkaProducer<String, String> producer =
        new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());
    closeables.add(producer);
    return producer;
  }

  /**
   * Sends {@code record} and waits for its acknowledgement, again while a broker has not yet heard
   * of the ACLs that allow it.
   */
  private static void send(
      KafkaProducer<String, String> producer, ProducerRecord<String, String> record)
      throws Exception {
    Instant deadline = Instant.now().plus(WAIT);
    while (true) {
      try {
        producer.send(record).get();
        return;
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof AuthorizationException) || Instant.now().isAfter(deadline)) {
          throw e;
        }
        Thread.sleep(100);
      }
    }
  }

  private static AclBinding acl(String principal, AclOperation operation) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
        new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW));
  }

  // --- the cluster --------------------------------------------------------------------------

  /** Two partitions on both brokers, broker 0 leading partition 0 and broker 1 partition 1. */
  private static void createTopic(Admin admin, String name) throws Exception {
    admin
        .createTopics(List.of(new NewTopic(name, Map.of(0, List.of(0, 1), 1, List.of(1, 0)))))
        .all()
        .get();
    awaitIsr(admin, name);
  }

  /** Waits until every partition of {@code topic} has its first replica leading, both in sync. */
  private static void awaitIsr(Admin admin, String topic) throws InterruptedException {
    eventually(
        () -> {
          try {
            for (TopicPartitionInfo partition : describe(admin, topic).partitions()) {
              if (partition.leader() == null
                  || partition.leader().id() != partition.replicas().get(0).id()
                  || partition.isr().size() != 2) {
                return false;
              }
            }
            return true;
          } catch (ExecutionException e) {
            return false;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
        },
        "both replicas of " + topic + " in sync");
  }

  /** {@code describeTopics}, by reflection across 3.0 and 4.x (see BrokerTest). */
  private static TopicDescription describe(Admin admin, String topic)
      throws ExecutionException, InterruptedException {
    DescribeTopicsResult result = admin.describeTopics(List.of(topic));
    Method all;
    try {
      all = DescribeTopicsResult.class.getMethod("allTopicNames");
    } catch (NoSuchMethodException e) {
      try {
        all = DescribeTopicsResult.class.getMethod("all");
      } catch (NoSuchMethodException none) {
        throw new AssertionError(none);
      }
    }
    try {
      @SuppressWarnings("unchecked")
      KafkaFuture<Map<String, TopicDescription>> future =
          (KafkaFuture<Map<String, TopicDescription>>) all.invoke(result);
      TopicDescription description = future.get().get(topic);
      assertEquals(topic, description.name());
      return description;
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
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
