package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException.Reason;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.junit.jupiter.api.Test;

class SpiffeTrustManagerTest {

  private static final TestPki.Authority OWN = TestPki.root("example.org");
  private static final TestPki.Authority FEDERATED = TestPki.root("k8s.example.org");
  private static final BundleSet BUNDLES =
      BundleSet.of(
          List.of(
              new X509Bundle(TrustDomain.parse("example.org"), List.of(OWN.certificate())),
              new X509Bundle(
                  TrustDomain.parse("k8s.example.org"), List.of(FEDERATED.certificate()))));
  private static final SpiffeIdMatcher EVERY_TRUST_DOMAIN =
      SpiffeIdMatcher.none().withTrustDomains(List.of("example.org", "k8s.example.org"));

  private static SpiffeTrustManager manager(SpiffeIdMatcher peers, Instant now) {
    return new SpiffeTrustManager(BUNDLES, peers, Clock.fixed(now, ZoneOffset.UTC));
  }

  private static SpiffeTrustManager manager() {
    return manager(EVERY_TRUST_DOMAIN, TestPki.NOW);
  }

  private static X509Certificate[] chain(TestPki.Issued issued) {
    return issued.chain().toArray(X509Certificate[]::new);
  }

  private static Reason refusal(
      SpiffeTrustManager manager, X509Certificate[] chain, boolean server) {
    return assertThrows(PeerRejectedException.class, () -> manager.verify(chain, server)).reason();
  }

  @Test
  void acceptsAPeerOfEachTrustDomain() throws CertificateException {
    assertEquals(
        SpiffeId.parse("spiffe://example.org/broker"),
        manager().verify(chain(OWN.svid("/broker")), true));
    assertEquals(
        SpiffeId.parse("spiffe://k8s.example.org/ns/a/sa/b"),
        manager().verify(chain(FEDERATED.svid("/ns/a/sa/b")), false));
  }

  @Test
  void acceptsAChainThroughIntermediates() throws CertificateException {
    TestPki.Issued issued = OWN.intermediate().intermediate().svid("/a");
    assertEquals(3, issued.chain().size());
    manager().verify(chain(issued), false);
  }

  @Test
  void acceptsAChainThatIncludesItsRoot() throws CertificateException {
    List<X509Certificate> withRoot = new ArrayList<>(OWN.intermediate().svid("/a").chain());
    withRoot.add(OWN.certificate());
    manager().verify(withRoot.toArray(X509Certificate[]::new), false);
  }

  @Test
  void refusesAFederatedAuthorityVouchingForAnotherTrustDomain() {
    TestPki.Issued forged = FEDERATED.leaf().uris("spiffe://example.org/broker").issue();
    assertEquals(Reason.CHAIN, refusal(manager(), chain(forged), true));
  }

  @Test
  void refusesAnUnknownTrustDomain() {
    TestPki.Issued stranger = TestPki.root("other.example.org").svid("/a");
    assertEquals(
        Reason.UNKNOWN_TRUST_DOMAIN,
        refusal(
            manager(
                SpiffeIdMatcher.none().withTrustDomains(List.of("other.example.org")), TestPki.NOW),
            chain(stranger),
            false));
  }

  @Test
  void refusesAChainOfAnUnrelatedAuthorityOfTheSameName() {
    TestPki.Issued impostor = TestPki.root("example.org").svid("/broker");
    assertEquals(Reason.CHAIN, refusal(manager(), chain(impostor), true));
  }

  @Test
  void refusesAChainMissingItsIntermediate() {
    TestPki.Issued issued = OWN.intermediate().svid("/a");
    assertEquals(Reason.CHAIN, refusal(manager(), new X509Certificate[] {issued.leaf()}, false));
  }

  @Test
  void checksValidityAtTheClocksTime() throws CertificateException {
    X509Certificate[] chain = chain(OWN.svid("/a"));
    manager(EVERY_TRUST_DOMAIN, TestPki.NOW.plus(Duration.ofMinutes(29))).verify(chain, false);
    assertEquals(
        Reason.CHAIN,
        refusal(
            manager(EVERY_TRUST_DOMAIN, TestPki.NOW.plus(Duration.ofMinutes(31))), chain, false));
    assertEquals(
        Reason.CHAIN,
        refusal(
            manager(EVERY_TRUST_DOMAIN, TestPki.NOW.minus(Duration.ofMinutes(31))), chain, false));
  }

  @Test
  void refusesWhatIsNotAnSvid() {
    assertEquals(Reason.MALFORMED_SVID, refusal(manager(), new X509Certificate[0], false));
    assertEquals(Reason.MALFORMED_SVID, refusal(manager(), null, false));
    assertEquals(Reason.MALFORMED_SVID, refusal(manager(), chain(OWN.leaf().issue()), false));
    assertEquals(
        Reason.MALFORMED_SVID,
        refusal(manager(), chain(OWN.leaf().uris("spiffe://example.org/a").ca().issue()), false));
  }

  @Test
  void refusesAnIdTheAllowListDoesNotCover() throws CertificateException {
    SpiffeTrustManager onlyBroker =
        manager(
            SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")), TestPki.NOW);
    onlyBroker.verify(chain(OWN.svid("/broker")), true);
    assertEquals(Reason.ID_NOT_ALLOWED, refusal(onlyBroker, chain(OWN.svid("/other")), true));
  }

  @Test
  void anEmptyAllowListAllowsNobody() {
    assertEquals(
        Reason.ID_NOT_ALLOWED,
        refusal(manager(SpiffeIdMatcher.none(), TestPki.NOW), chain(OWN.svid("/a")), true));
  }

  @Test
  void checksTheExtendedKeyUsageForThePurpose() throws CertificateException {
    X509Certificate[] serverOnly =
        chain(
            OWN.leaf()
                .uris("spiffe://example.org/a")
                .purposes(KeyPurposeId.id_kp_serverAuth)
                .issue());
    manager().verify(serverOnly, true);
    assertEquals(Reason.CHAIN, refusal(manager(), serverOnly, false));

    X509Certificate[] clientOnly =
        chain(
            OWN.leaf()
                .uris("spiffe://example.org/a")
                .purposes(KeyPurposeId.id_kp_clientAuth)
                .issue());
    manager().verify(clientOnly, false);
    assertEquals(Reason.CHAIN, refusal(manager(), clientOnly, true));

    X509Certificate[] none = chain(OWN.leaf().uris("spiffe://example.org/a").purposes().issue());
    manager().verify(none, true);
    manager().verify(none, false);

    X509Certificate[] any =
        chain(
            OWN.leaf()
                .uris("spiffe://example.org/a")
                .purposes(KeyPurposeId.anyExtendedKeyUsage)
                .issue());
    manager().verify(any, true);
  }

  @Test
  void namesNoAcceptedIssuers() {
    assertEquals(0, manager().getAcceptedIssuers().length);
  }

  @Test
  void theJdkEntryPointsVerifyToo() {
    X509Certificate[] forged = chain(FEDERATED.leaf().uris("spiffe://example.org/broker").issue());
    SpiffeTrustManager manager = manager();
    assertThrows(CertificateException.class, () -> manager.checkServerTrusted(forged, "EC"));
    assertThrows(CertificateException.class, () -> manager.checkClientTrusted(forged, "EC"));
    assertThrows(
        CertificateException.class,
        () -> manager.checkServerTrusted(forged, "EC", (javax.net.ssl.SSLEngine) null));
    assertThrows(
        CertificateException.class,
        () -> manager.checkClientTrusted(forged, "EC", (java.net.Socket) null));
  }

  /** Algorithm constraints that forbid what {@code forbidden} names: an algorithm or key type. */
  private static javax.net.ssl.SSLEngine engineForbidding(String forbidden) throws Exception {
    javax.net.ssl.SSLEngine engine = javax.net.ssl.SSLContext.getDefault().createSSLEngine();
    javax.net.ssl.SSLParameters parameters = engine.getSSLParameters();
    parameters.setAlgorithmConstraints(
        new java.security.AlgorithmConstraints() {
          @Override
          public boolean permits(
              java.util.Set<java.security.CryptoPrimitive> primitives,
              String algorithm,
              java.security.AlgorithmParameters parameters) {
            return !algorithm.equalsIgnoreCase(forbidden);
          }

          @Override
          public boolean permits(
              java.util.Set<java.security.CryptoPrimitive> primitives, java.security.Key key) {
            return !key.getAlgorithm().equalsIgnoreCase(forbidden);
          }

          @Override
          public boolean permits(
              java.util.Set<java.security.CryptoPrimitive> primitives,
              String algorithm,
              java.security.Key key,
              java.security.AlgorithmParameters parameters) {
            return permits(primitives, algorithm, parameters) && permits(primitives, key);
          }
        });
    engine.setSSLParameters(parameters);
    return engine;
  }

  @Test
  void appliesTheConnectionsAlgorithmConstraints() throws Exception {
    X509Certificate[] chain = chain(OWN.intermediate().svid("/broker"));
    SpiffeTrustManager manager = manager();
    manager.checkServerTrusted(chain, "EC", engineForbidding("SHA1withRSA"));

    PeerRejectedException signature =
        assertThrows(
            PeerRejectedException.class,
            () -> manager.checkServerTrusted(chain, "EC", engineForbidding("SHA256withECDSA")));
    assertEquals(Reason.CHAIN, signature.reason());

    PeerRejectedException key =
        assertThrows(
            PeerRejectedException.class,
            () -> manager.checkClientTrusted(chain, "EC", engineForbidding("EC")));
    assertEquals(Reason.CHAIN, key.reason());
  }

  @Test
  void withoutAConnectionNoConstraintsAreAdded() throws CertificateException {
    X509Certificate[] chain = chain(OWN.svid("/broker"));
    manager().checkServerTrusted(chain, "EC", (java.net.Socket) null);
    manager().checkServerTrusted(chain, "EC", (javax.net.ssl.SSLEngine) null);
  }

  @Test
  void tellsTheObserverWhatEachHandshakeVerificationCameTo() throws CertificateException {
    List<String> seen = new ArrayList<>();
    SpiffeTrustManager.Observer observer =
        new SpiffeTrustManager.Observer() {
          @Override
          public void accepted(boolean server, SpiffeId id) {
            seen.add((server ? "server " : "client ") + id);
          }

          @Override
          public void rejected(boolean server, PeerRejectedException rejection) {
            seen.add((server ? "server " : "client ") + rejection.reason());
          }
        };
    SpiffeTrustManager manager =
        new SpiffeTrustManager(
            BUNDLES,
            SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker")),
            Clock.fixed(TestPki.NOW, ZoneOffset.UTC),
            observer);

    manager.checkServerTrusted(chain(OWN.svid("/broker")), "EC", (javax.net.ssl.SSLEngine) null);
    X509Certificate[] outsider = chain(FEDERATED.svid("/ns/a/sa/b"));
    assertThrows(PeerRejectedException.class, () -> manager.checkClientTrusted(outsider, "EC"));
    X509Certificate[] unknown = chain(TestPki.root("other.org").svid("/x"));
    assertThrows(
        PeerRejectedException.class,
        () -> manager.checkClientTrusted(unknown, "EC", (java.net.Socket) null));
    // verify() is not a handshake: it tells the observer nothing.
    manager.verify(chain(OWN.svid("/broker")), true);

    assertEquals(
        List.of(
            "server spiffe://example.org/broker",
            "client " + Reason.ID_NOT_ALLOWED,
            "client " + Reason.UNKNOWN_TRUST_DOMAIN),
        seen);
  }
}
