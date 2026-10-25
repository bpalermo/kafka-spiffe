package io.github.bpalermo.kafka.spiffe.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.github.bpalermo.kafka.spiffe.workload.X509MaterialSource;
import io.github.bpalermo.kafka.spiffe.workload.X509SourceRegistry;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.netty.channel.epoll.Epoll;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Engine pairs, no broker: a broker's and a client's factory, each on a fake Workload API of its
 * own, handshaking in memory (design, section 9).
 */
class SpiffeSslEngineFactoryTest {

  private static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  private static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");
  private static final Duration WAIT = Duration.ofSeconds(10);

  /** A clock a test moves. */
  private static final class MovableClock extends Clock {
    private volatile Instant now = TestPki.NOW;

    void set(Instant instant) {
      now = instant;
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  private final MovableClock clock = new MovableClock();
  private Path directory;
  private FakeWorkloadApi brokerAgent;
  private FakeWorkloadApi clientAgent;
  private X509SourceRegistry registry;
  private final List<SpiffeSslEngineFactory> factories = new java.util.ArrayList<>();

  @BeforeEach
  void agents() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    directory = Files.createTempDirectory(Path.of("/tmp"), "ef");
    brokerAgent = FakeWorkloadApi.start(directory.resolve("broker.sock"));
    clientAgent = FakeWorkloadApi.start(directory.resolve("client.sock"));
    brokerAgent.publish(response(BROKER_CA.svid("/broker"), BROKER_CA));
    clientAgent.publish(response(CLIENT_CA.svid("/ns/a/sa/b"), CLIENT_CA));
    registry = new X509SourceRegistry(clock);
  }

  @AfterEach
  void stop() throws Exception {
    factories.forEach(SpiffeSslEngineFactory::close);
    if (brokerAgent != null) {
      brokerAgent.close();
      clientAgent.close();
      Files.deleteIfExists(directory);
    }
  }

  /** A response with both trust domains' bundles, as federated agents deliver them. */
  private static FakeWorkloadApi.Response response(TestPki.Issued issued, TestPki.Authority own) {
    TestPki.Authority other = own == BROKER_CA ? CLIENT_CA : BROKER_CA;
    String otherDomain = own == BROKER_CA ? "k8s.example.org" : "example.org";
    return new FakeWorkloadApi.Response()
        .svid(FakeWorkloadApi.Svid.of(issued, own))
        .federated("spiffe://" + otherDomain, List.of(other.certificate()));
  }

  private SpiffeSslEngineFactory factory(Map<String, Object> configs) {
    SpiffeSslEngineFactory factory =
        new SpiffeSslEngineFactory(domain -> registry, clock, Map.of());
    factory.configure(configs);
    factories.add(factory);
    return factory;
  }

  private Map<String, Object> brokerConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("spiffe.endpoint.socket", "unix://" + brokerAgent.socket());
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    configs.put("spiffe.peer.allowed.ids", "spiffe://example.org/broker");
    configs.put("ssl.client.auth", "required");
    configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    configs.put("spiffe.startup.timeout.ms", "10000");
    return configs;
  }

  private Map<String, Object> clientConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("spiffe.endpoint.socket", "unix://" + clientAgent.socket());
    configs.put("spiffe.peer.allowed.ids", "spiffe://example.org/broker");
    configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
    configs.put("spiffe.startup.timeout.ms", "10000");
    return configs;
  }

  @Test
  void aSourceAcquiredWithTheSameKeysIsTheFactorysOwn() throws Exception {
    // The factory Kafka makes, on the process's shared registries and the system clock, so the
    // SVID is valid now.
    Instant now = Instant.now();
    clientAgent.publish(
        response(
            CLIENT_CA
                .leaf()
                .uris("spiffe://k8s.example.org/ns/a/sa/b")
                .validity(now.minus(Duration.ofHours(1)), now.plus(Duration.ofHours(1)))
                .issue(),
            CLIENT_CA));
    Map<String, Object> configs = clientConfigs();
    configs.put("spiffe.metrics.jmx.enabled", "false");
    SpiffeSslEngineFactory factory = new SpiffeSslEngineFactory();
    factory.configure(configs);
    factories.add(factory);
    X509SourceRegistry shared = X509SourceRegistry.shared(null);
    int running = shared.size();
    try (X509MaterialSource source = SpiffeConfig.parse(configs, Map.of()).acquireSource()) {
      assertEquals(running, shared.size());
      assertEquals(
          "spiffe://k8s.example.org/ns/a/sa/b",
          source.awaitFirst(Duration.ofSeconds(10)).svid().id().toString());
      assertEquals(1, clientAgent.calls());
      assertTrue(source.stats().updates() >= 1);
    }
    // The factory still holds the source.
    assertEquals(running, shared.size());
    factory.close();
    assertEquals(running - 1, shared.size());
  }

  private static Handshake.Sessions connect(
      SpiffeSslEngineFactory client, SpiffeSslEngineFactory broker) throws SSLException {
    return Handshake.run(
        client.createClientSslEngine("10.0.0.1", 9093, "https"),
        broker.createServerSslEngine("10.0.0.2", 50000));
  }

  private static X509Certificate leaf(java.security.cert.Certificate[] chain) {
    return (X509Certificate) chain[0];
  }

  private static void eventually(java.util.function.BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(10);
    }
  }

  @Test
  void aClientAndABrokerVerifyEachOtherBySpiffeId() throws Exception {
    Handshake.Sessions sessions = connect(factory(clientConfigs()), factory(brokerConfigs()));
    assertTrue(
        leaf(sessions.client().getPeerCertificates())
            .getSubjectAlternativeNames()
            .toString()
            .contains("spiffe://example.org/broker"));
    assertTrue(
        leaf(sessions.server().getPeerCertificates())
            .getSubjectAlternativeNames()
            .toString()
            .contains("spiffe://k8s.example.org/ns/a/sa/b"));
  }

  @Test
  void aBrokerOutsideTheClientsAllowListIsRefused() {
    Map<String, Object> client = clientConfigs();
    client.put("spiffe.peer.allowed.ids", "spiffe://example.org/other-broker");
    SpiffeSslEngineFactory clientFactory = factory(client);
    SpiffeSslEngineFactory brokerFactory = factory(brokerConfigs());
    assertThrows(SSLException.class, () -> connect(clientFactory, brokerFactory));
  }

  @Test
  void aClientOutsideTheBrokersAllowListIsRefused() {
    Map<String, Object> broker = brokerConfigs();
    broker.put("spiffe.peer.allowed.trust.domains", "other.example.org");
    SpiffeSslEngineFactory clientFactory = factory(clientConfigs());
    SpiffeSslEngineFactory brokerFactory = factory(broker);
    assertThrows(SSLException.class, () -> connect(clientFactory, brokerFactory));
  }

  @Test
  void aRotationTakesEffectOnTheNextHandshakeWithoutRebuildingAndOpenEnginesKeepTheirs()
      throws Exception {
    SpiffeSslEngineFactory client = factory(clientConfigs());
    SpiffeSslEngineFactory broker = factory(brokerConfigs());
    X509Certificate before = leaf(connect(client, broker).client().getPeerCertificates());

    SSLEngine openedBefore = broker.createServerSslEngine("10.0.0.2", 50001);
    brokerAgent.publish(response(BROKER_CA.svid("/broker"), BROKER_CA));
    eventually(
        () -> {
          try {
            return !leaf(connect(client, broker).client().getPeerCertificates()).equals(before);
          } catch (SSLException e) {
            throw new AssertionError(e);
          }
        },
        "the broker to present its rotated SVID");

    X509Certificate fromOldEngine =
        leaf(
            Handshake.run(client.createClientSslEngine("10.0.0.1", 9093, "https"), openedBefore)
                .client()
                .getPeerCertificates());
    assertEquals(before, fromOldEngine);
  }

  @Test
  void theInterBrokerValidationHandshakeNeedsTheBrokersOwnIdAllowed() throws Exception {
    SpiffeSslEngineFactory broker = factory(brokerConfigs());
    Handshake.run(broker.createClientSslEngine("", 0, null), broker.createServerSslEngine("", 0));

    Map<String, Object> withoutItself = brokerConfigs();
    withoutItself.remove("spiffe.peer.allowed.ids");
    SpiffeSslEngineFactory stranger = factory(withoutItself);
    assertThrows(
        SSLException.class,
        () ->
            Handshake.run(
                stranger.createClientSslEngine("", 0, null),
                stranger.createServerSslEngine("", 0)));
  }

  @Test
  void aBrokerRequiresClientCertificatesUnlessToldOtherwise() {
    Map<String, Object> none = brokerConfigs();
    none.put("ssl.client.auth", "none");
    assertThrows(ConfigException.class, () -> factory(none));

    none.put("spiffe.server.allow.unauthenticated", "true");
    SSLEngine relaxed = factory(none).createServerSslEngine("", 0);
    assertFalse(relaxed.getNeedClientAuth());

    Map<String, Object> requested = brokerConfigs();
    requested.put("ssl.client.auth", "requested");
    requested.put("spiffe.server.allow.unauthenticated", "true");
    assertTrue(factory(requested).createServerSslEngine("", 0).getWantClientAuth());

    assertTrue(factory(brokerConfigs()).createServerSslEngine("", 0).getNeedClientAuth());
  }

  @Test
  void enginesNeverIdentifyTheEndpointAndUseTheConfiguredProtocols() {
    Map<String, Object> configs = clientConfigs();
    configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, List.of("TLSv1.3"));
    configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
    SSLEngine engine = factory(configs).createClientSslEngine("10.0.0.1", 9093, "https");
    assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    assertEquals(List.of("TLSv1.3"), List.of(engine.getEnabledProtocols()));
    assertTrue(engine.getUseClientMode());
  }

  @Test
  void startupFailsAfterTheTimeoutAndReleasesTheSource() throws Exception {
    Path silent = directory.resolve("silent.sock");
    try (FakeWorkloadApi silentAgent = FakeWorkloadApi.start(silent)) {
      assertEquals(silent, silentAgent.socket());
      Map<String, Object> configs = clientConfigs();
      configs.put("spiffe.endpoint.socket", "unix://" + silent);
      configs.put("spiffe.startup.timeout.ms", "300");
      SpiffeSslEngineFactory factory =
          new SpiffeSslEngineFactory(domain -> registry, clock, Map.of());
      KafkaException e = assertThrows(KafkaException.class, () -> factory.configure(configs));
      assertTrue(e.getMessage().contains("no X.509 SVID"), e.getMessage());
      assertEquals(0, registry.size());
    }
  }

  @Test
  void anExpiredSvidIsPresentedByDefaultOrRefusedUnderFail() {
    SpiffeSslEngineFactory present = factory(clientConfigs());
    Map<String, Object> failing = clientConfigs();
    failing.put("spiffe.svid.expired", "fail");
    SpiffeSslEngineFactory fail = factory(failing);
    clock.set(TestPki.NOW.plus(Duration.ofHours(1)));
    present.createClientSslEngine("10.0.0.1", 9093, "https");
    KafkaException e =
        assertThrows(
            KafkaException.class, () -> fail.createClientSslEngine("10.0.0.1", 9093, "https"));
    assertTrue(e.getMessage().contains("expired"), e.getMessage());
  }

  @Test
  void factoriesShareTheSourceAndRebuildOnlyForTheReconfigurableKeys() {
    SpiffeSslEngineFactory one = factory(brokerConfigs());
    factory(brokerConfigs());
    assertEquals(1, registry.size());

    Map<String, Object> next = new HashMap<>(brokerConfigs());
    assertFalse(one.shouldBeRebuilt(next));
    next.put("spiffe.peer.allowed.ids", "spiffe://example.org/changed");
    assertFalse(one.shouldBeRebuilt(next), "spiffe.* keys are not reconfigurable");
    next.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, List.of("TLS_AES_128_GCM_SHA256"));
    assertTrue(one.shouldBeRebuilt(next));

    Map<String, Object> spelled = new HashMap<>(brokerConfigs());
    spelled.put("ssl.client.auth", " REQUIRED ");
    assertFalse(one.shouldBeRebuilt(spelled));
    spelled.put("ssl.client.auth", "requested");
    assertTrue(one.shouldBeRebuilt(spelled));

    assertEquals(SpiffeSslEngineFactory.RECONFIGURABLE, one.reconfigurableConfigs());
    assertNull(one.keystore());
    assertNull(one.truststore());
  }

  @Test
  void closingReleasesTheSource() {
    SpiffeSslEngineFactory factory = factory(clientConfigs());
    assertEquals(1, registry.size());
    factory.close();
    factory.close();
    assertEquals(0, registry.size());
    assertNotEquals(null, factory.reconfigurableConfigs());
  }

  @Test
  void aFactoryKafkaDropsWithoutClosingReleasesTheSourceOnceCollected() throws Exception {
    factory(clientConfigs());
    unclosedFactory();
    assertEquals(1, registry.size());
    // A rotation must not need the dropped factory, or keep it reachable.
    clientAgent.publish(response(CLIENT_CA.svid("/ns/a/sa/b"), CLIENT_CA));
    factories.forEach(SpiffeSslEngineFactory::close);
    factories.clear();
    eventually(
        () -> {
          System.gc();
          return registry.size() == 0;
        },
        "the dropped factory's handle to be released");
  }

  /** Configures a factory and drops it without closing it, as Kafka's validation does. */
  private void unclosedFactory() {
    new SpiffeSslEngineFactory(domain -> registry, clock, Map.of()).configure(clientConfigs());
  }

  @Test
  void emptyProtocolAndCipherListsMeanTheJdkDefaultsAsInKafkasOwnFactory() {
    Map<String, Object> configs = clientConfigs();
    configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, List.of());
    configs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "");
    SSLEngine engine = factory(configs).createClientSslEngine("10.0.0.1", 9093, "https");
    SSLEngine plain = factory(clientConfigs()).createClientSslEngine("10.0.0.1", 9093, "https");
    assertEquals(List.of(plain.getEnabledProtocols()), List.of(engine.getEnabledProtocols()));
    assertEquals(List.of(plain.getEnabledCipherSuites()), List.of(engine.getEnabledCipherSuites()));
  }

  @Test
  void aStaleCallerCannotRollTheFactoryBack() throws Exception {
    SpiffeSslEngineFactory client = factory(clientConfigs());
    SpiffeSslEngineFactory broker = factory(brokerConfigs());
    TestPki.Issued last = null;
    for (int i = 0; i < 3; i++) {
      last = BROKER_CA.svid("/broker");
      brokerAgent.publish(response(last, BROKER_CA));
    }
    X509Certificate newest = last.leaf();
    // Rotations arrive while engines are made from other threads; whatever the interleaving, every
    // engine made after the last rotation is installed presents the newest SVID.
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(4);
    try {
      for (int i = 0; i < 50; i++) {
        pool.submit(() -> broker.createServerSslEngine("10.0.0.2", 50000));
      }
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
    }
    eventually(
        () -> {
          try {
            return newest.equals(leaf(connect(client, broker).client().getPeerCertificates()));
          } catch (SSLException e) {
            throw new AssertionError(e);
          }
        },
        "the last published SVID");
    for (int i = 0; i < 10; i++) {
      assertEquals(newest, leaf(connect(client, broker).client().getPeerCertificates()));
    }
  }

  @Test
  void withTheMBeansOffHandshakesAreCountedAllTheSame() throws Exception {
    Map<String, Object> client = clientConfigs();
    client.put("spiffe.metrics.jmx.enabled", "false");
    Map<String, Object> broker = brokerConfigs();
    broker.put("spiffe.metrics.jmx.enabled", "false");
    Map<String, Object> strict = brokerConfigs();
    strict.remove("spiffe.peer.allowed.trust.domains");
    strict.put("spiffe.metrics.jmx.enabled", "false");
    // Read before any factory of the domain exists: the view is of the counts the factories use.
    PeerVerificationStats stats = PeerVerificationStats.of(null);
    long serverAccepted = stats.server().accepted();
    long serverRefused = stats.server().rejected().get(PeerRejectedException.Reason.ID_NOT_ALLOWED);
    long clientAccepted = stats.client().accepted();
    SpiffeSslEngineFactory clientFactory = factory(client);
    SpiffeSslEngineFactory brokerFactory = factory(broker);
    SpiffeSslEngineFactory strictFactory = factory(strict);

    connect(clientFactory, brokerFactory);
    assertThrows(SSLException.class, () -> connect(clientFactory, strictFactory));

    assertEquals(serverAccepted + 1, stats.server().accepted());
    assertEquals(
        serverRefused + 1,
        stats.server().rejected().get(PeerRejectedException.Reason.ID_NOT_ALLOWED));
    assertEquals(clientAccepted + 2, stats.client().accepted());
    assertFalse(
        java.lang.management.ManagementFactory.getPlatformMBeanServer()
            .queryNames(new javax.management.ObjectName("*:type=peer-verification,*"), null)
            .stream()
            .anyMatch(name -> name.getDomain().isEmpty() || "null".equals(name.getDomain())));
  }

  /**
   * A TLS probe that sends no certificate, as a monitoring check of the broker's own certificate
   * does: the JDK refuses it before any trust manager runs, so the peer-verification counts do not
   * move.
   */
  @Test
  void aClientWithNoCertificateIsRefusedByTheJdkAndNotCounted() throws Exception {
    String domain = "test.no.certificate";
    Map<String, Object> broker = brokerConfigs();
    broker.put("spiffe.metrics.jmx.domain", domain);
    SpiffeSslEngineFactory brokerFactory = factory(broker);
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance(protocol);
      context.init(null, new javax.net.ssl.TrustManager[] {new TrustingEveryone()}, null);
      SSLEngine probe = context.createSSLEngine("10.0.0.2", 9093);
      probe.setUseClientMode(true);
      probe.setEnabledProtocols(new String[] {protocol});
      SSLException refused =
          assertThrows(
              SSLException.class,
              () -> Handshake.run(probe, brokerFactory.createServerSslEngine("10.0.0.1", 50000)));
      assertTrue(
          String.valueOf(refused.getMessage()).contains("Empty client certificate chain"),
          protocol + ": " + refused);
    }
    PeerVerificationStats stats = PeerVerificationStats.of(domain);
    assertEquals(0, stats.server().accepted());
    assertEquals(0, stats.server().rejectedTotal());
  }

  /** The probe checks nothing of the broker's certificate. */
  private static final class TrustingEveryone implements javax.net.ssl.X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  @Test
  void handshakesAreCountedInThePeerVerificationMBeans() throws Exception {
    // A JMX domain of this test's own: the counts are the process's, per domain.
    String domain = "test.peer.verification";
    Map<String, Object> client = clientConfigs();
    client.put("spiffe.metrics.jmx.domain", domain);
    Map<String, Object> broker = brokerConfigs();
    broker.put("spiffe.metrics.jmx.domain", domain);
    // A broker that allows only brokers, so it refuses the client.
    Map<String, Object> strict = brokerConfigs();
    strict.remove("spiffe.peer.allowed.trust.domains");
    strict.put("spiffe.metrics.jmx.domain", domain);
    SpiffeSslEngineFactory clientFactory = factory(client);
    SpiffeSslEngineFactory brokerFactory = factory(broker);
    SpiffeSslEngineFactory strictFactory = factory(strict);

    connect(clientFactory, brokerFactory);
    assertThrows(SSLException.class, () -> connect(clientFactory, strictFactory));

    javax.management.MBeanServer server =
        java.lang.management.ManagementFactory.getPlatformMBeanServer();
    javax.management.ObjectName asServer =
        new javax.management.ObjectName(domain + ":type=peer-verification,mode=server");
    javax.management.ObjectName asClient =
        new javax.management.ObjectName(domain + ":type=peer-verification,mode=client");
    assertEquals(1L, server.getAttribute(asServer, "accepted-total"));
    assertEquals(1L, server.getAttribute(asServer, "rejected-total"));
    assertEquals(1L, server.getAttribute(asServer, "rejected-id-not-allowed-total"));
    assertEquals(0L, server.getAttribute(asServer, "rejected-chain-total"));
    // The client verified both brokers before the strict one refused it.
    assertEquals(2L, server.getAttribute(asClient, "accepted-total"));
    assertEquals(0L, server.getAttribute(asClient, "rejected-total"));
    // The same numbers without JMX.
    PeerVerificationStats stats = PeerVerificationStats.of(domain);
    assertEquals(1, stats.server().accepted());
    assertEquals(1, stats.server().rejectedTotal());
    assertEquals(1, stats.server().rejected().get(PeerRejectedException.Reason.ID_NOT_ALLOWED));
    assertEquals(0, stats.server().rejected().get(PeerRejectedException.Reason.CHAIN));
    assertEquals(2, stats.client().accepted());
    assertEquals(0, stats.client().rejectedTotal());

    // Registered while a factory of the domain is open.
    clientFactory.close();
    brokerFactory.close();
    assertTrue(server.isRegistered(asServer));
    strictFactory.close();
    assertFalse(server.isRegistered(asServer));
    assertFalse(server.isRegistered(asClient));
  }
}
