package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.Test;

/** Handshakes between two snapshot contexts, in memory: what the engine factory will build on. */
class SnapshotSslContextTest {

  private static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  private static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");
  private static final List<X509Bundle> BOTH =
      List.of(
          new X509Bundle(TrustDomain.parse("example.org"), List.of(BROKER_CA.certificate())),
          new X509Bundle(TrustDomain.parse("k8s.example.org"), List.of(CLIENT_CA.certificate())));
  private static final Clock CLOCK = Clock.fixed(TestPki.NOW, ZoneOffset.UTC);

  private static SSLContext context(TestPki.Issued issued, SpiffeIdMatcher peers, String protocol)
      throws GeneralSecurityException {
    return context(issued, peers, protocol, CLOCK);
  }

  private static SSLContext context(
      TestPki.Issued issued, SpiffeIdMatcher peers, String protocol, Clock clock)
      throws GeneralSecurityException {
    X509Context snapshot =
        new X509Context(X509Svid.of(issued.chain(), issued.key(), ""), BundleSet.of(BOTH));
    return SnapshotSslContext.create(
        snapshot, peers, new SnapshotSslContext.Settings(protocol, null), clock);
  }

  private static SSLContext broker(TestPki.Issued issued) throws GeneralSecurityException {
    return context(
        issued, SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")), "TLS");
  }

  private static SSLContext client(TestPki.Issued issued) throws GeneralSecurityException {
    return context(
        issued, SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")), "TLS");
  }

  /** The reason the handshake failed, which one side's trust manager gave. */
  private static PeerRejectedException.Reason refusal(SSLContext client, SSLContext server) {
    SSLException e = assertThrows(SSLException.class, () -> Handshake.run(client, server));
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof PeerRejectedException rejected) {
        return rejected.reason();
      }
    }
    throw new AssertionError("the handshake failed for another reason", e);
  }

  private static SpiffeId idOf(Certificate[] chain) throws CertificateException {
    return SvidLeaf.spiffeId((X509Certificate) chain[0]);
  }

  @Test
  void aFederatedClientAndABrokerVerifyEachOther() throws Exception {
    Handshake.Sessions sessions =
        Handshake.run(
            client(CLIENT_CA.intermediate().svid("/ns/a/sa/b")),
            broker(BROKER_CA.intermediate().svid("/broker")));
    assertEquals(
        SpiffeId.parse("spiffe://example.org/broker"),
        idOf(sessions.client().getPeerCertificates()));
    assertEquals(
        SpiffeId.parse("spiffe://k8s.example.org/ns/a/sa/b"),
        idOf(sessions.server().getPeerCertificates()));
  }

  @Test
  void worksWithRsaKeysAndOnTls12() throws Exception {
    TestPki.Issued rsaClient = CLIENT_CA.leaf().uris("spiffe://k8s.example.org/c").rsa().issue();
    TestPki.Issued rsaBroker = BROKER_CA.leaf().uris("spiffe://example.org/broker").rsa().issue();
    Handshake.run(client(rsaClient), broker(rsaBroker));
    Handshake.Sessions tls12 =
        Handshake.run(
            context(
                CLIENT_CA.svid("/c"),
                SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
                "TLSv1.2"),
            context(
                BROKER_CA.svid("/broker"),
                SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")),
                "TLSv1.2"));
    assertEquals("TLSv1.2", tls12.client().getProtocol());
  }

  @Test
  void theClientRefusesABrokerItDoesNotAllow() throws Exception {
    SSLContext client = client(CLIENT_CA.svid("/c"));
    SSLContext impostor = broker(BROKER_CA.svid("/not-the-broker"));
    assertEquals(PeerRejectedException.Reason.ID_NOT_ALLOWED, refusal(client, impostor));
  }

  @Test
  void theBrokerRefusesAClientOfATrustDomainItDoesNotAllow() throws Exception {
    SSLContext stranger = client(BROKER_CA.svid("/c"));
    SSLContext broker = broker(BROKER_CA.svid("/broker"));
    assertEquals(PeerRejectedException.Reason.ID_NOT_ALLOWED, refusal(stranger, broker));
  }

  @Test
  void aForgedCrossTrustDomainSvidIsRefused() throws Exception {
    SSLContext client = client(CLIENT_CA.svid("/c"));
    SSLContext forged = broker(CLIENT_CA.leaf().uris("spiffe://example.org/broker").issue());
    assertEquals(PeerRejectedException.Reason.CHAIN, refusal(client, forged));
  }

  @Test
  void endpointIdentificationIsIgnored() throws Exception {
    SSLContext client = client(CLIENT_CA.svid("/c"));
    var engine = client.createSSLEngine("10.0.0.1", 9093);
    engine.setUseClientMode(true);
    var parameters = engine.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    engine.setSSLParameters(parameters);
    var server = broker(BROKER_CA.svid("/broker")).createSSLEngine();
    server.setUseClientMode(false);
    server.setNeedClientAuth(true);
    Handshake.Sessions sessions = Handshake.run(engine, server);
    assertEquals(
        SpiffeId.parse("spiffe://example.org/broker"),
        idOf(sessions.client().getPeerCertificates()));
  }

  @Test
  void aClientWithoutACertificateIsRefusedWhenOneIsRequired() throws Exception {
    SSLContext anonymous = SSLContext.getInstance("TLS");
    anonymous.init(
        null,
        new javax.net.ssl.TrustManager[] {
          new SpiffeTrustManager(
              BundleSet.of(BOTH),
              SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
              CLOCK)
        },
        null);
    SSLContext broker = broker(BROKER_CA.svid("/broker"));
    assertThrows(SSLException.class, () -> Handshake.run(anonymous, broker));
  }

  /** A client engine that caches its session for the broker at a host and port, as Kafka's do. */
  private static Handshake.Sessions toBroker(SSLContext client, SSLContext broker)
      throws SSLException {
    var clientEngine = client.createSSLEngine("broker.example.org", 9093);
    clientEngine.setUseClientMode(true);
    var brokerEngine = broker.createSSLEngine();
    brokerEngine.setUseClientMode(false);
    brokerEngine.setNeedClientAuth(true);
    return Handshake.run(clientEngine, brokerEngine);
  }

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

  /**
   * A reconnection within the session timeout is resumed, so the engines verify the peer
   * themselves; a full handshake's peer is verified by the trust manager alone.
   */
  @Test
  void aReconnectionWithinTheTimeoutIsResumedAndVerifiedByTheEngines() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      SSLContext client =
          context(
              CLIENT_CA.svid("/c"),
              SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
              protocol);
      SSLContext broker =
          context(
              BROKER_CA.svid("/broker"),
              SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")),
              protocol);
      VerifyingSslEngine[] full = engines(client, broker);
      Handshake.run(full[0], full[1]);
      assertFalse(full[0].verifiedAgain(), protocol + ": the client's first handshake");
      assertFalse(full[1].verifiedAgain(), protocol + ": the broker's first handshake");

      VerifyingSslEngine[] resumed = engines(client, broker);
      Handshake.run(resumed[0], resumed[1]);
      assertTrue(resumed[0].verifiedAgain(), protocol + ": the client resumed");
      assertTrue(resumed[1].verifiedAgain(), protocol + ": the broker resumed");
    }
  }

  /** A client engine and a broker engine, as {@link #toBroker} makes them. */
  private static VerifyingSslEngine[] engines(SSLContext client, SSLContext broker) {
    SSLEngine clientEngine = client.createSSLEngine("broker.example.org", 9093);
    clientEngine.setUseClientMode(true);
    SSLEngine brokerEngine = broker.createSSLEngine();
    brokerEngine.setUseClientMode(false);
    brokerEngine.setNeedClientAuth(true);
    return new VerifyingSslEngine[] {
      (VerifyingSslEngine) clientEngine, (VerifyingSslEngine) brokerEngine
    };
  }

  /**
   * A reconnection within the session timeout is resumed, and a resumption skips the JDK's
   * certificate checks; the client verifies the broker's SVID again all the same, so once that has
   * expired by its clock, the reconnection is refused.
   */
  @Test
  void theClientVerifiesAResumedSessionAgain() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      MovableClock clientClock = new MovableClock();
      SSLContext client =
          context(
              CLIENT_CA.svid("/c"),
              SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
              protocol,
              clientClock);
      SSLContext broker =
          context(
              BROKER_CA.svid("/broker"),
              SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")),
              protocol);
      toBroker(client, broker);

      clientClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
      assertEquals(
          PeerRejectedException.Reason.CHAIN,
          refusalOf(() -> toBroker(client, broker)),
          protocol + ": a session was resumed after its peer's SVID expired");
    }
  }

  /** The same on the broker's side, against a client whose SVID has expired by its clock. */
  @Test
  void theBrokerVerifiesAResumedSessionAgain() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      MovableClock brokerClock = new MovableClock();
      SSLContext client =
          context(
              CLIENT_CA.svid("/c"),
              SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
              protocol);
      SSLContext broker =
          context(
              BROKER_CA.svid("/broker"),
              SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")),
              protocol,
              brokerClock);
      toBroker(client, broker);

      brokerClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
      assertEquals(
          PeerRejectedException.Reason.CHAIN,
          refusalOf(() -> toBroker(client, broker)),
          protocol + ": a session was resumed after its peer's SVID expired");
    }
  }

  private interface HandshakeAttempt {
    void run() throws SSLException;
  }

  private static PeerRejectedException.Reason refusalOf(HandshakeAttempt attempt) {
    SSLException e = assertThrows(SSLException.class, attempt::run);
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof PeerRejectedException rejected) {
        return rejected.reason();
      }
    }
    throw new AssertionError("the handshake failed for another reason", e);
  }

  @Test
  void settingsAreChecked() {
    assertThrows(IllegalArgumentException.class, () -> new SnapshotSslContext.Settings("", null));
  }
}
