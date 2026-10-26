package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
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

  /** How many tries a test makes for the JDK to resume a session within its one second. */
  private static final int TRIES = 5;

  /** Counts what a context's trust manager and engines tell it. */
  private static final class Counting implements SpiffeTrustManager.Observer {
    private final AtomicInteger accepted = new AtomicInteger();
    private final AtomicInteger rejected = new AtomicInteger();

    @Override
    public void accepted(boolean server, SpiffeId id) {
      accepted.incrementAndGet();
    }

    @Override
    public void rejected(boolean server, PeerRejectedException rejection) {
      rejected.incrementAndGet();
    }
  }

  private static SSLContext context(
      TestPki.Issued issued,
      SpiffeIdMatcher peers,
      String protocol,
      Clock clock,
      SpiffeTrustManager.Observer observer)
      throws GeneralSecurityException {
    X509Context snapshot =
        new X509Context(X509Svid.of(issued.chain(), issued.key(), ""), BundleSet.of(BOTH));
    return SnapshotSslContext.create(
        snapshot, peers, new SnapshotSslContext.Settings(protocol, null), clock, observer);
  }

  private static SSLContext clientContext(String protocol, Clock clock, Counting observer)
      throws GeneralSecurityException {
    return context(
        CLIENT_CA.svid("/c"),
        SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
        protocol,
        clock,
        observer);
  }

  private static SSLContext brokerContext(String protocol, Clock clock, Counting observer)
      throws GeneralSecurityException {
    return context(
        BROKER_CA.svid("/broker"),
        SpiffeIdMatcher.none().withTrustDomains(List.of("k8s.example.org")),
        protocol,
        clock,
        observer);
  }

  /**
   * A reconnection within the session timeout is resumed, so the engines verify the peer
   * themselves, and each verification is counted once; a full handshake's peer is verified by the
   * trust manager alone, and counted once.
   */
  @Test
  void aReconnectionWithinTheTimeoutIsResumedAndVerifiedByTheEngines() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      boolean resumedOnce = false;
      for (int attempt = 0; attempt < TRIES && !resumedOnce; attempt++) {
        Counting clientCounts = new Counting();
        Counting brokerCounts = new Counting();
        SSLContext client = clientContext(protocol, CLOCK, clientCounts);
        SSLContext broker = brokerContext(protocol, CLOCK, brokerCounts);
        VerifyingSslEngine[] full = engines(client, broker);
        Handshake.run(full[0], full[1]);
        assertFalse(full[0].verifiedAgain(), protocol + ": the client's first handshake");
        assertFalse(full[1].verifiedAgain(), protocol + ": the broker's first handshake");
        assertEquals(1, clientCounts.accepted.get(), protocol + ": the client's first count");
        assertEquals(1, brokerCounts.accepted.get(), protocol + ": the broker's first count");

        VerifyingSslEngine[] again = engines(client, broker);
        Handshake.run(again[0], again[1]);
        if (!again[0].verifiedAgain()) {
          continue; // More than a second passed: a full handshake. Try again.
        }
        resumedOnce = true;
        assertTrue(again[1].verifiedAgain(), protocol + ": the broker resumed too");
        assertEquals(2, clientCounts.accepted.get(), protocol + ": the client's counts");
        assertEquals(2, brokerCounts.accepted.get(), protocol + ": the broker's counts");
        assertEquals(0, clientCounts.rejected.get() + brokerCounts.rejected.get());
      }
      assertTrue(resumedOnce, protocol + ": the JDK never resumed a session");
    }
  }

  /**
   * {@code beginHandshake} on an established connection: on TLS 1.3 it is a KeyUpdate, which
   * verifies nobody, so the connection goes on although the peer's SVID has since expired by the
   * client's clock; on TLS 1.2 it is a renegotiation, a handshake whose peer is verified again, and
   * refused.
   */
  @Test
  void beginningAHandshakeAgainChecksTheRenegotiationOfTls12ButNotTheKeyUpdateOfTls13()
      throws Exception {
    for (String protocol : List.of("TLSv1.3", "TLSv1.2")) {
      MovableClock clientClock = new MovableClock();
      SSLContext client = clientContext(protocol, clientClock, new Counting());
      SSLContext broker = brokerContext(protocol, CLOCK, new Counting());
      VerifyingSslEngine[] connection = engines(client, broker);
      Handshake.run(connection[0], connection[1]);
      exchange(connection[0], connection[1]);
      clientClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
      if (protocol.equals("TLSv1.3")) {
        rehandshake(connection[0], connection[1]);
        exchange(connection[0], connection[1]);
      } else {
        SSLException e =
            assertThrows(
                SSLException.class,
                () -> {
                  rehandshake(connection[0], connection[1]);
                  exchange(connection[0], connection[1]);
                });
        assertEquals(PeerRejectedException.Reason.CHAIN, reasonOf(e));
      }
    }
  }

  /** The client begins a handshake again, and both engines go on until neither has more to do. */
  private static void rehandshake(SSLEngine client, SSLEngine broker) throws SSLException {
    client.beginHandshake();
    ByteBuffer toBroker = ByteBuffer.allocate(client.getSession().getPacketBufferSize() * 4);
    ByteBuffer toClient = ByteBuffer.allocate(broker.getSession().getPacketBufferSize() * 4);
    ByteBuffer empty = ByteBuffer.allocate(0);
    ByteBuffer sink = ByteBuffer.allocate(client.getSession().getApplicationBufferSize());
    for (int round = 0; round < 100; round++) {
      boolean idle =
          client.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
              && broker.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
              && toBroker.position() == 0
              && toClient.position() == 0;
      if (idle) {
        return;
      }
      pump(client, toBroker, toClient, empty, sink);
      pump(broker, toClient, toBroker, empty, sink);
    }
    throw new AssertionError("the handshake did not settle");
  }

  private static void pump(
      SSLEngine engine, ByteBuffer out, ByteBuffer in, ByteBuffer empty, ByteBuffer sink)
      throws SSLException {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }
    if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
      engine.wrap(empty, out);
    }
    if (in.position() > 0) {
      in.flip();
      sink.clear();
      engine.unwrap(in, sink);
      in.compact();
    }
  }

  /** A record of application data each way. */
  private static void exchange(SSLEngine client, SSLEngine broker) throws SSLException {
    send(client, broker);
    send(broker, client);
  }

  private static void send(SSLEngine from, SSLEngine to) throws SSLException {
    ByteBuffer net = ByteBuffer.allocate(from.getSession().getPacketBufferSize());
    from.wrap(ByteBuffer.wrap("ping".getBytes(StandardCharsets.US_ASCII)), net);
    net.flip();
    ByteBuffer plain = ByteBuffer.allocate(to.getSession().getApplicationBufferSize());
    while (net.hasRemaining()) {
      to.unwrap(net, plain);
    }
    assertEquals(4, plain.position(), "the record arrived");
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
   * certificate checks; the client's engine verifies the broker's SVID again all the same, so once
   * that has expired by its clock, the resumed session is refused.
   */
  @Test
  void theClientVerifiesAResumedSessionAgain() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      assertEquals(
          PeerRejectedException.Reason.CHAIN,
          refusedOnResumption(
              protocol,
              () -> {
                MovableClock clientClock = new MovableClock();
                SSLContext client = clientContext(protocol, clientClock, new Counting());
                SSLContext broker = brokerContext(protocol, CLOCK, new Counting());
                toBroker(client, broker);
                clientClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
                return () -> toBroker(client, broker);
              }),
          protocol);
    }
  }

  /** The same on the broker's side, against a client whose SVID has expired by its clock. */
  @Test
  void theBrokerVerifiesAResumedSessionAgain() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      assertEquals(
          PeerRejectedException.Reason.CHAIN,
          refusedOnResumption(
              protocol,
              () -> {
                MovableClock brokerClock = new MovableClock();
                SSLContext client = clientContext(protocol, CLOCK, new Counting());
                SSLContext broker = brokerContext(protocol, brokerClock, new Counting());
                toBroker(client, broker);
                brokerClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
                return () -> toBroker(client, broker);
              }),
          protocol);
    }
  }

  /**
   * The client refuses a resumed session in the {@code wrap} that would send its Finished. Kafka
   * 3.0 clears its buffer before a wrap and flips it after, but not when the wrap throws, and then
   * sends the buffer from its position to its limit: none of the refused record may be there.
   */
  @Test
  void aResumptionRefusedOnWrapLeavesNothingToSendInABufferUsedAsKafka30Does() throws Exception {
    for (String protocol : List.of("TLSv1.2", "TLSv1.3")) {
      boolean refusedOnce = false;
      for (int attempt = 0; attempt < TRIES && !refusedOnce; attempt++) {
        MovableClock clientClock = new MovableClock();
        SSLContext client = clientContext(protocol, clientClock, new Counting());
        SSLContext broker = brokerContext(protocol, CLOCK, new Counting());
        toBroker(client, broker);
        clientClock.set(TestPki.NOW.plus(Duration.ofHours(2)));
        VerifyingSslEngine[] again = engines(client, broker);
        ByteBuffer clientOut = ByteBuffer.allocate(again[0].getSession().getPacketBufferSize());
        // What an earlier record left in the buffer, which a wrong limit would send.
        while (clientOut.hasRemaining()) {
          clientOut.put((byte) 0x55);
        }
        SSLException e =
            assertThrows(SSLException.class, () -> handshakeAsKafka30(again, clientOut));
        if (!resumed(e)) {
          continue;
        }
        refusedOnce = true;
        assertEquals(PeerRejectedException.Reason.CHAIN, reasonOf(e), protocol);
        assertEquals(0, clientOut.remaining(), protocol + ": bytes left to send");
      }
      assertTrue(refusedOnce, protocol + ": the JDK never resumed a session");
    }
  }

  /**
   * Runs a handshake in which the client's wraps go through {@code clientOut} as Kafka 3.0's
   * SslTransportLayer uses its buffer: cleared before, flipped after, left as it is on a throw.
   */
  private static void handshakeAsKafka30(VerifyingSslEngine[] engines, ByteBuffer clientOut)
      throws SSLException {
    SSLEngine client = engines[0];
    SSLEngine broker = engines[1];
    int packet = client.getSession().getPacketBufferSize();
    ByteBuffer toBroker = ByteBuffer.allocate(packet * 4);
    ByteBuffer toClient = ByteBuffer.allocate(packet * 4);
    ByteBuffer brokerOut = ByteBuffer.allocate(packet);
    ByteBuffer empty = ByteBuffer.allocate(0);
    ByteBuffer sink = ByteBuffer.allocate(client.getSession().getApplicationBufferSize());
    client.beginHandshake();
    broker.beginHandshake();
    for (int round = 0; round < 100; round++) {
      step(client, clientOut, toBroker, toClient, empty, sink);
      step(broker, brokerOut, toClient, toBroker, empty, sink);
    }
    throw new AssertionError("the client did not refuse the resumed session");
  }

  private static void step(
      SSLEngine engine,
      ByteBuffer out,
      ByteBuffer peer,
      ByteBuffer in,
      ByteBuffer empty,
      ByteBuffer sink)
      throws SSLException {
    SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
    while (status == SSLEngineResult.HandshakeStatus.NEED_TASK) {
      Runnable task;
      while ((task = engine.getDelegatedTask()) != null) {
        task.run();
      }
      status = engine.getHandshakeStatus();
    }
    if (status == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
      out.clear();
      engine.wrap(empty, out);
      out.flip();
      peer.put(out);
    } else if (status == SSLEngineResult.HandshakeStatus.NEED_UNWRAP && in.position() > 0) {
      in.flip();
      sink.clear();
      engine.unwrap(in, sink);
      in.compact();
    }
  }

  /**
   * When the peer is verified again on the first application data an unwrap decrypts (should the
   * JDK not report the handshake finished), a refusal takes that data back out of every destination
   * buffer.
   */
  @Test
  void aRefusalOnTheFirstApplicationDataTakesItBackFromEveryBuffer() throws Exception {
    Handshake.Sessions sessions =
        Handshake.run(client(CLIENT_CA.svid("/c")), broker(BROKER_CA.svid("/broker")));
    MovableClock later = new MovableClock();
    later.set(TestPki.NOW.plus(Duration.ofHours(2)));
    SpiffeTrustManager trust =
        new SpiffeTrustManager(
            BundleSet.of(BOTH),
            SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
            later);
    VerifyingSslEngine engine =
        new VerifyingSslEngine(new Decrypting(sessions.client()), trust, new AtomicBoolean());
    ByteBuffer first = ByteBuffer.allocate(16);
    ByteBuffer second = ByteBuffer.allocate(64);
    SSLException e =
        assertThrows(
            SSLException.class,
            () -> engine.unwrap(ByteBuffer.allocate(8), new ByteBuffer[] {first, second}));
    assertEquals(PeerRejectedException.Reason.CHAIN, reasonOf(e));
    assertEquals(0, first.position());
    assertEquals(0, second.position());
    assertArrayEquals(new byte[16], first.array());
    assertArrayEquals(new byte[64], second.array());
    // And it is thrown again on the next read.
    assertThrows(
        SSLException.class,
        () -> engine.unwrap(ByteBuffer.allocate(8), new ByteBuffer[] {first, second}));
  }

  /**
   * An engine whose handshake is done, as far as anyone can tell, and whose unwrap decrypts the
   * peer's first application data across two buffers, without reporting the handshake finished.
   */
  private static final class Decrypting extends SSLEngine {
    private static final byte[] DATA =
        "the first application data of the peer".getBytes(StandardCharsets.US_ASCII);
    private final SSLSession session;

    Decrypting(SSLSession session) {
      this.session = session;
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) {
      dsts[offset].put(DATA, 0, 10);
      dsts[offset + 1].put(DATA, 10, DATA.length - 10);
      int consumed = src.remaining();
      src.position(src.limit());
      return new SSLEngineResult(
          SSLEngineResult.Status.OK,
          SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
          consumed,
          DATA.length);
    }

    @Override
    public SSLSession getSession() {
      return session;
    }

    @Override
    public boolean getUseClientMode() {
      return true;
    }

    @Override
    public SSLParameters getSSLParameters() {
      return new SSLParameters();
    }

    @Override
    public void closeOutbound() {}

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable getDelegatedTask() {
      return null;
    }

    @Override
    public void closeInbound() {}

    @Override
    public boolean isInboundDone() {
      return false;
    }

    @Override
    public boolean isOutboundDone() {
      return false;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return new String[0];
    }

    @Override
    public String[] getEnabledCipherSuites() {
      return new String[0];
    }

    @Override
    public void setEnabledCipherSuites(String[] suites) {}

    @Override
    public String[] getSupportedProtocols() {
      return new String[0];
    }

    @Override
    public String[] getEnabledProtocols() {
      return new String[0];
    }

    @Override
    public void setEnabledProtocols(String[] protocols) {}

    @Override
    public void beginHandshake() {}

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
      return SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
    }

    @Override
    public void setUseClientMode(boolean mode) {}

    @Override
    public void setNeedClientAuth(boolean need) {}

    @Override
    public boolean getNeedClientAuth() {
      return false;
    }

    @Override
    public void setWantClientAuth(boolean want) {}

    @Override
    public boolean getWantClientAuth() {
      return false;
    }

    @Override
    public void setEnableSessionCreation(boolean flag) {}

    @Override
    public boolean getEnableSessionCreation() {
      return false;
    }
  }

  private interface HandshakeAttempt {
    void run() throws SSLException;
  }

  /** Makes the contexts, runs a first handshake, and returns the reconnection to try. */
  private interface Setup {
    HandshakeAttempt prepare() throws Exception;
  }

  /**
   * The reason a reconnection was refused, once it was resumed and refused by an engine; a
   * reconnection more than a second later is a full handshake, so it is tried again.
   */
  private static PeerRejectedException.Reason refusedOnResumption(String protocol, Setup setup)
      throws Exception {
    for (int attempt = 0; attempt < TRIES; attempt++) {
      HandshakeAttempt reconnection = setup.prepare();
      SSLException e = assertThrows(SSLException.class, reconnection::run);
      if (resumed(e)) {
        return reasonOf(e);
      }
    }
    throw new AssertionError(protocol + ": the JDK never resumed a session");
  }

  /** Whether an engine refused a resumed session, rather than a trust manager a full handshake. */
  private static boolean resumed(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (String.valueOf(t.getMessage()).contains("the peer of a resumed session is refused")) {
        return true;
      }
    }
    return false;
  }

  private static PeerRejectedException.Reason reasonOf(Throwable e) {
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
