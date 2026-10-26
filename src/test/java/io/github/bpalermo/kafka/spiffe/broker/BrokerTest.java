package io.github.bpalermo.kafka.spiffe.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder;
import io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory;
import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A cluster of two brokers in this process, on the Kafka version and mode of the target (design,
 * section 9, "Kafka"): KRaft, with a controller, before StandardAuthorizer, or ZooKeeper, before
 * AclAuthorizer. The brokers' listener and the inter-broker one are TLS through {@link
 * SpiffeSslEngineFactory} and {@link SpiffePrincipalBuilder}, and every broker and client takes its
 * SVIDs from a Workload API of its own.
 *
 * <p>The brokers are {@value #BROKER} of example.org, the clients workloads of k8s.example.org;
 * each trusts the other's trust domain through a federated bundle. A KRaft controller listens in
 * plaintext (see {@link BrokerCluster}), where the brokers are {@code User:ANONYMOUS}, a super
 * user.
 */
class BrokerTest {

  private static final String BROKER = "spiffe://example.org/broker";
  private static final String ADMIN = "spiffe://k8s.example.org/ns/ops/sa/admin";
  private static final String APP = "spiffe://k8s.example.org/ns/a/sa/b";

  /** Verified in the handshake, but outside the principal allow-list. */
  private static final String OUTSIDER = "spiffe://k8s.example.org/batch/job";

  /** Inside it, to show when an ACL has reached both brokers. */
  private static final String CONTROL = "spiffe://k8s.example.org/ns/control/sa/c";

  private static final String BROKER_DOMAIN = "kafka.spiffe";
  private static final String CLIENT_DOMAIN = "kafka.spiffe.client";
  private static final Duration WAIT = Duration.ofSeconds(60);

  /**
   * Who issues each side's SVIDs and which roots each side's bundle holds. Only the rotation test
   * changes them, and leaves every SVID and bundle consistent.
   */
  private static TestPki.Authority brokerIssuer = TestPki.root("example.org");

  private static TestPki.Authority clientIssuer = TestPki.root("k8s.example.org");
  private static List<X509Certificate> brokerRoots = List.of(brokerIssuer.certificate());
  private static List<X509Certificate> clientRoots = List.of(clientIssuer.certificate());

  private static Path directory;
  private static FakeWorkloadApi brokerAgent;
  private static BrokerCluster cluster;

  private final List<AutoCloseable> closeables = new ArrayList<>();
  private int agents;

  @BeforeAll
  static void cluster() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    assertEquals(
        System.getProperty("kafka.spiffe.expected.kafka.version"),
        kafkaVersion(),
        "the Kafka on the class path");
    directory = Files.createTempDirectory(Path.of("/tmp"), "kraft");
    brokerAgent = FakeWorkloadApi.start(directory.resolve("broker.sock"));
    brokerAgent.publish(brokerResponse());

    Map<String, String> configs = new HashMap<>();
    configs.put(
        "listener.security.protocol.map", BrokerCluster.LISTENER + ":SSL,CONTROLLER:PLAINTEXT");
    configs.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    configs.put("ssl.client.auth", "required");
    configs.put("principal.builder.class", SpiffePrincipalBuilder.class.getName());
    configs.put("spiffe.endpoint.socket", "unix://" + brokerAgent.socket());
    configs.put("spiffe.startup.timeout.ms", "10000");
    // The brokers verify each other, and the clients of k8s.example.org.
    configs.put("spiffe.peer.allowed.ids", BROKER);
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    // Principals for the brokers and the service accounts; OUTSIDER gets spiffe-unauthorized.
    configs.put("spiffe.principal.allowed.ids", BROKER);
    configs.put("spiffe.principal.allowed.id.patterns", "spiffe://k8s.example.org/ns/*/sa/*");
    // Assigned before it starts, so that teardown stops what a failed start leaves running.
    cluster = BrokerCluster.named();
    String superUsers = "Spiffe:" + BROKER + ";Spiffe:" + ADMIN;
    if (cluster.kraft()) {
      configs.put(
          "authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
      configs.put("super.users", superUsers + ";User:ANONYMOUS");
    } else {
      configs.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
      configs.put("super.users", superUsers);
    }
    configs.put("offsets.topic.replication.factor", "2");
    configs.put("offsets.topic.num.partitions", "2");
    configs.put("transaction.state.log.replication.factor", "2");
    configs.put("transaction.state.log.min.isr", "1");
    configs.put("group.initial.rebalance.delay.ms", "0");
    cluster.start(2, configs);
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
  void spiffePrincipalsAreAuthorizedByAcls() throws Exception {
    Admin admin = admin(agent(ADMIN));
    // On KRaft, CreateTopics and CreateAcls are forwarded to the controller with the admin's
    // principal, which the controller reads back through the builder's serde and finds a super
    // user; on ZooKeeper the broker authorizes them and writes to ZooKeeper.
    createTopic(admin, "orders");
    AclBinding write = acl(APP, ResourceType.TOPIC, "orders", AclOperation.WRITE);
    AclBinding read = acl(APP, ResourceType.TOPIC, "orders", AclOperation.READ);
    AclBinding group = acl(APP, ResourceType.GROUP, "orders-reader", AclOperation.READ);
    admin.createAcls(List.of(write, read, group)).all().get();

    FakeWorkloadApi app = agent(APP);
    KafkaProducer<String, String> producer = producer(app);
    for (int i = 0; i < 10; i++) {
      send(producer, new ProducerRecord<>("orders", "k" + i, "v" + i));
    }
    KafkaConsumer<String, String> consumer = consumer(app, Map.of("group.id", "orders-reader"));
    consumer.subscribe(List.of("orders"));
    assertEquals(10, consume(consumer, 10).size());

    Set<String> principals = new HashSet<>();
    admin
        .describeAcls(AclBindingFilter.ANY)
        .values()
        .get()
        .forEach(b -> principals.add(b.entry().principal()));
    assertTrue(principals.contains("Spiffe:" + APP), principals.toString());
  }

  @Test
  void aPeerOutsideThePrincipalAllowListGetsAuthorizationErrors() throws Exception {
    Admin owner = admin(agent(ADMIN));
    createTopic(owner, "payments");
    // An ACL for the ID itself lets nothing in: the peer's principal is spiffe-unauthorized.
    owner
        .createAcls(List.of(acl(OUTSIDER, ResourceType.TOPIC, "payments", AclOperation.WRITE)))
        .all()
        .get();
    // The brokers apply the metadata log (or ZooKeeper's ACL change notifications) in order: once
    // a later ACL lets CONTROL write to the partition each broker leads, both have OUTSIDER's.
    owner
        .createAcls(List.of(acl(CONTROL, ResourceType.TOPIC, "payments", AclOperation.WRITE)))
        .all()
        .get();
    KafkaProducer<String, String> control = producer(agent(CONTROL));
    for (int partition = 0; partition < 2; partition++) {
      send(control, new ProducerRecord<>("payments", partition, "k", "v"));
    }

    FakeWorkloadApi outsider = agent(OUTSIDER);
    KafkaProducer<String, String> producer = producer(outsider);
    for (int partition = 0; partition < 2; partition++) {
      ProducerRecord<String, String> record = new ProducerRecord<>("payments", partition, "k", "v");
      ExecutionException produced =
          assertThrows(ExecutionException.class, () -> producer.send(record).get());
      assertInstanceOf(TopicAuthorizationException.class, produced.getCause());
    }

    // As Spiffe:spiffe-unauthorized, which is no super user: on KRaft forwarded to the controller
    // through the serde, on ZooKeeper refused by the broker.
    Admin admin = admin(outsider);
    ExecutionException created =
        assertThrows(
            ExecutionException.class,
            () -> admin.createTopics(List.of(new NewTopic("refused", 1, (short) 1))).all().get());
    assertInstanceOf(AuthorizationException.class, created.getCause());
  }

  @Test
  void aPeerOutsideTheBrokersAllowListIsRefusedInTheHandshakeAndCounted() throws Exception {
    String domain = "other.example.org";
    TestPki.Authority stranger = TestPki.root(domain);
    String id = "spiffe://" + domain + "/ns/a/sa/b";
    long rejected = attribute(BROKER_DOMAIN, "server", "rejected-unknown-trust-domain-total");
    assertTrue(attribute(BROKER_DOMAIN, "server", "accepted-total") > 0, "inter-broker handshakes");

    FakeWorkloadApi agent = newAgent();
    agent.publish(
        new FakeWorkloadApi.Response()
            .svid(
                new FakeWorkloadApi.Svid(
                    id, svid(stranger, id), List.of(stranger.certificate()), ""))
            .federated("spiffe://example.org", brokerRoots));
    Admin admin = admin(agent);
    ExecutionException refused =
        assertThrows(ExecutionException.class, () -> admin.describeCluster().clusterId().get());
    assertInstanceOf(SslAuthenticationException.class, refused.getCause());
    eventually(
        () -> attribute(BROKER_DOMAIN, "server", "rejected-unknown-trust-domain-total") > rejected,
        "the refusal counted by the brokers");
    // The stranger verified the broker before the broker refused it.
    assertTrue(attribute(CLIENT_DOMAIN, "client", "accepted-total") > 0);
  }

  @Test
  void replicasFollowOverTheTlsInterBrokerListener() throws Exception {
    Admin admin = admin(agent(ADMIN));
    NewTopic topic =
        new NewTopic("replicated", 2, (short) 2).configs(Map.of("min.insync.replicas", "2"));
    admin.createTopics(List.of(topic)).all().get();
    awaitIsr(admin, "replicated");
    KafkaProducer<String, String> producer = producer(agent(ADMIN));
    // acks=all with min.insync.replicas=2: each write waits for the other broker's fetch.
    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<>("replicated", "k" + i, "v" + i)).get();
    }
    awaitIsr(admin, "replicated");
  }

  /**
   * Rotations of the brokers' and the client's SVIDs, then a move of both trust domains to new
   * roots, the old ones dropped at the end, while a producer and a consumer go on with the
   * connections they opened before any of it.
   */
  @Test
  void rotationsTakeEffectWithoutNewConnections() throws Exception {
    Admin admin = admin(agent(ADMIN));
    createTopic(admin, "rotation");
    admin
        .createAcls(
            List.of(
                acl(APP, ResourceType.TOPIC, "rotation", AclOperation.WRITE),
                acl(APP, ResourceType.TOPIC, "rotation", AclOperation.READ)))
        .all()
        .get();

    FakeWorkloadApi app = agent(APP);
    KafkaProducer<String, String> producer = producer(app);
    KafkaConsumer<String, String> consumer = consumer(app, Map.of());
    consumer.assign(List.of(new TopicPartition("rotation", 0), new TopicPartition("rotation", 1)));
    consumer.seekToBeginning(consumer.assignment());
    round(producer, consumer);
    round(producer, consumer);
    double connections = connections(producer.metrics()) + connections(consumer.metrics());
    assertTrue(connections > 0, "the clients count their connections");

    // New SVIDs from the same roots.
    rotate(app);
    round(producer, consumer);

    // New roots join the bundles, then issue the SVIDs, then the old ones leave.
    TestPki.Authority newBrokerIssuer = TestPki.root("example.org");
    TestPki.Authority newClientIssuer = TestPki.root("k8s.example.org");
    List<X509Certificate> oldBrokerRoots = brokerRoots;
    List<X509Certificate> oldClientRoots = clientRoots;
    TestPki.Authority oldClientIssuer = clientIssuer;
    brokerRoots = List.of(oldBrokerRoots.get(0), newBrokerIssuer.certificate());
    clientRoots = List.of(oldClientRoots.get(0), newClientIssuer.certificate());
    rotate(app);
    round(producer, consumer);
    brokerIssuer = newBrokerIssuer;
    clientIssuer = newClientIssuer;
    rotate(app);
    round(producer, consumer);
    brokerRoots = List.of(newBrokerIssuer.certificate());
    clientRoots = List.of(newClientIssuer.certificate());
    rotate(app);
    round(producer, consumer);

    assertEquals(
        connections,
        connections(producer.metrics()) + connections(consumer.metrics()),
        "connection-creation-total of the producer and the consumer");

    // A new client trusting only the new roots: the brokers present the new SVID, and trust the
    // new root of k8s.example.org.
    KafkaProducer<String, String> fresh = producer(agent(APP));
    fresh.send(new ProducerRecord<>("rotation", "k", "v")).get();

    // One whose SVID the old root issued is refused.
    long chain = attribute(BROKER_DOMAIN, "server", "rejected-chain-total");
    FakeWorkloadApi old = newAgent();
    old.publish(
        new FakeWorkloadApi.Response()
            .svid(new FakeWorkloadApi.Svid(APP, svid(oldClientIssuer, APP), oldClientRoots, ""))
            .federated("spiffe://example.org", brokerRoots));
    Admin refused = admin(old);
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> refused.describeCluster().clusterId().get());
    assertInstanceOf(SslAuthenticationException.class, e.getCause());
    eventually(
        () -> attribute(BROKER_DOMAIN, "server", "rejected-chain-total") > chain,
        "the refusal counted by the brokers");
  }

  /** The version of the kafka-clients on the class path, as it records it. */
  private static String kafkaVersion() throws IOException {
    Properties properties = new Properties();
    try (InputStream in = BrokerTest.class.getResourceAsStream("/kafka/kafka-version.properties")) {
      properties.load(in);
    }
    return properties.getProperty("version");
  }

  // --- the PKI ------------------------------------------------------------------------------

  /** An SVID valid for an hour around now, as the brokers' and clients' clocks are the system's. */
  private static TestPki.Issued svid(TestPki.Authority issuer, String id) {
    Instant now = Instant.now();
    return issuer
        .leaf()
        .uris(id)
        .validity(now.minus(Duration.ofMinutes(5)), now.plusSeconds(3600))
        .issue();
  }

  private static FakeWorkloadApi.Response brokerResponse() {
    return new FakeWorkloadApi.Response()
        .svid(new FakeWorkloadApi.Svid(BROKER, svid(brokerIssuer, BROKER), brokerRoots, ""))
        .federated("spiffe://k8s.example.org", clientRoots);
  }

  private static FakeWorkloadApi.Response clientResponse(String id) {
    return new FakeWorkloadApi.Response()
        .svid(new FakeWorkloadApi.Svid(id, svid(clientIssuer, id), clientRoots, ""))
        .federated("spiffe://example.org", brokerRoots);
  }

  /** Publishes the current PKI to the brokers and {@code app}, and waits until both have it. */
  private static void rotate(FakeWorkloadApi app) throws Exception {
    long brokers = updates(BROKER_DOMAIN, brokerAgent);
    long client = updates(CLIENT_DOMAIN, app);
    brokerAgent.publish(brokerResponse());
    app.publish(clientResponse(APP));
    eventually(
        () -> updates(BROKER_DOMAIN, brokerAgent) > brokers && updates(CLIENT_DOMAIN, app) > client,
        "the brokers' and the client's sources take the update");
  }

  // --- clients ------------------------------------------------------------------------------

  private FakeWorkloadApi newAgent() throws Exception {
    FakeWorkloadApi agent =
        FakeWorkloadApi.start(directory.resolve("client-" + agents++ + ".sock"));
    closeables.add(agent);
    closeables.add(() -> Files.deleteIfExists(agent.socket()));
    return agent;
  }

  /** A Workload API serving {@code id} an SVID of the current client root. */
  private FakeWorkloadApi agent(String id) throws Exception {
    FakeWorkloadApi agent = newAgent();
    agent.publish(clientResponse(id));
    return agent;
  }

  private static Map<String, Object> clientConfigs(FakeWorkloadApi agent) {
    Map<String, Object> configs = new HashMap<>();
    configs.put("bootstrap.servers", cluster.bootstrapServers());
    configs.put("security.protocol", "SSL");
    configs.put("ssl.engine.factory.class", SpiffeSslEngineFactory.class.getName());
    configs.put("spiffe.endpoint.socket", "unix://" + agent.socket());
    configs.put("spiffe.peer.allowed.ids", BROKER);
    configs.put("spiffe.startup.timeout.ms", "10000");
    // The clients' MBeans apart from the brokers', which share this process.
    configs.put("spiffe.metrics.jmx.domain", CLIENT_DOMAIN);
    configs.put("default.api.timeout.ms", "30000");
    configs.put("request.timeout.ms", "20000");
    configs.put("max.block.ms", "30000");
    return configs;
  }

  private Admin admin(FakeWorkloadApi agent) {
    Admin admin = Admin.create(clientConfigs(agent));
    closeables.add(admin);
    return admin;
  }

  private KafkaProducer<String, String> producer(FakeWorkloadApi agent) {
    Map<String, Object> configs = clientConfigs(agent);
    configs.put("acks", "all");
    // Idempotence (the default from 3.0) has the producer ask a broker for a producer ID first,
    // which needs a write ACL there: refused by a broker that has not yet applied the ACL, or at
    // all for OUTSIDER, the producer is left in a fatal error state and send throws at once, which
    // send() below cannot retry. The records alone say what each broker allows (as in
    // MigrationTest); CrossVersionTest's client keeps the idempotent default.
    configs.put("enable.idempotence", "false");
    KafkaProducer<String, String> producer =
        new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());
    closeables.add(producer);
    return producer;
  }

  private KafkaConsumer<String, String> consumer(FakeWorkloadApi agent, Map<String, Object> more) {
    Map<String, Object> configs = clientConfigs(agent);
    configs.put("auto.offset.reset", "earliest");
    configs.put("enable.auto.commit", "false");
    configs.putAll(more);
    KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer());
    closeables.add(consumer);
    return consumer;
  }

  /**
   * Sends {@code record} and waits for its acknowledgement, again while a broker has not yet heard
   * of the ACLs that allow it: the controller (or ZooKeeper) has them before the brokers do.
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

  private static List<ConsumerRecord<String, String>> consume(
      KafkaConsumer<String, String> consumer, int count) {
    return consume(consumer, r -> true, count);
  }

  /**
   * Up to {@code count} records that pass {@code filter}; authorization errors are retried, as in
   * {@link #send}.
   */
  private static List<ConsumerRecord<String, String>> consume(
      KafkaConsumer<String, String> consumer,
      Predicate<ConsumerRecord<String, String>> filter,
      int count) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    Instant deadline = Instant.now().plus(WAIT);
    while (records.size() < count && Instant.now().isBefore(deadline)) {
      try {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
          if (filter.test(record)) {
            records.add(record);
          }
        }
      } catch (AuthorizationException e) {
        // The ACLs have not reached that broker yet.
      }
    }
    return records;
  }

  /** Five records produced and the same five consumed. */
  private static void round(
      KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer)
      throws Exception {
    String marker = Long.toString(System.nanoTime());
    // Both partitions, so both brokers, each the leader of one (createTopic).
    for (int i = 0; i < 5; i++) {
      send(producer, new ProducerRecord<>("rotation", i % 2, marker + i, marker));
    }
    assertEquals(
        5,
        consume(consumer, r -> marker.equals(r.value()), 5).size(),
        "records of round " + marker);
  }

  private static double connections(Map<MetricName, ? extends Metric> metrics) {
    double total = 0;
    for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
      MetricName name = metric.getKey();
      if (name.name().equals("connection-creation-total")
          && (name.group().equals("producer-metrics") || name.group().equals("consumer-metrics"))) {
        total += ((Number) metric.getValue().metricValue()).doubleValue();
      }
    }
    return total;
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

  /** Waits until every partition of {@code topic} has a leader and both replicas in sync. */
  private static void awaitIsr(Admin admin, String topic) throws Exception {
    eventually(
        () -> {
          try {
            TopicDescription description = describe(admin, topic);
            for (TopicPartitionInfo partition : description.partitions()) {
              if (partition.leader() == null || partition.isr().size() != 2) {
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

  /**
   * {@code describeTopics(...).all()}, which 4.0 removed for {@code allTopicNames()}, which 3.0
   * does not have; by reflection, as the test compiles against 3.0.
   */
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
      return future.get().get(topic);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static AclBinding acl(String id, ResourceType type, String name, AclOperation operation) {
    return new AclBinding(
        new ResourcePattern(type, name, PatternType.LITERAL),
        new AccessControlEntry("Spiffe:" + id, "*", operation, AclPermissionType.ALLOW));
  }

  // --- JMX ----------------------------------------------------------------------------------

  private static long attribute(String domain, String mode, String name) {
    return mbean(domain + ":type=peer-verification,mode=" + mode, name);
  }

  private static long updates(String domain, FakeWorkloadApi agent) {
    return mbean(
        domain + ":type=x509-source,socket=" + ObjectName.quote(agent.socket().toString()),
        "updates-total");
  }

  private static long mbean(String name, String attribute) {
    try {
      return ((Number)
              ManagementFactory.getPlatformMBeanServer()
                  .getAttribute(new ObjectName(name), attribute))
          .longValue();
    } catch (JMException e) {
      throw new AssertionError("no " + attribute + " of " + name, e);
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
