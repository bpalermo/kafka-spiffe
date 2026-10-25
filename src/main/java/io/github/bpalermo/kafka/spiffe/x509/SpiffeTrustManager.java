package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import java.io.IOException;
import java.net.Socket;
import java.security.AlgorithmConstraints;
import java.security.AlgorithmParameters;
import java.security.CryptoPrimitive;
import java.security.GeneralSecurityException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXCertPathValidatorResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Verifies a peer by its X.509-SVID, never by host name.
 *
 * <ol>
 *   <li>The leaf passes {@link SvidLeaf#check}, which yields the peer's SPIFFE ID.
 *   <li>The chain is validated by the JDK's PKIX validator against the bundle of the trust domain
 *       that ID names, and that bundle only: an authority federated with can never vouch for an
 *       identity of another trust domain. Revocation is not checked; SVIDs are short-lived instead.
 *       The JDK's {@code jdk.certpath.disabledAlgorithms} applies.
 *   <li>A leaf with an extended key usage must allow the purpose: {@code serverAuth} for a server,
 *       {@code clientAuth} for a client.
 *   <li>The ID must be one the allow-list matches.
 * </ol>
 *
 * <p>Endpoint identification is ignored, whatever the engine or socket asks for. Their algorithm
 * constraints ({@link SSLParameters#getAlgorithmConstraints()}) are applied to every certificate of
 * the chain and to the authority's key, since the JDK adds no checks around an {@link
 * X509ExtendedTrustManager}. The time is the clock's, so that tests can set it.
 *
 * <p>{@link #getAcceptedIssuers()} is empty: a server asks for a client certificate without naming
 * authorities, so a client whose SVID is issued by an intermediate the server does not hold still
 * presents it. Which authorities are accepted is decided per trust domain, here, not by the client.
 */
public final class SpiffeTrustManager extends X509ExtendedTrustManager {

  private static final String SERVER_AUTH = "1.3.6.1.5.5.7.3.1";
  private static final String CLIENT_AUTH = "1.3.6.1.5.5.7.3.2";
  private static final String ANY_EXTENDED_KEY_USAGE = "2.5.29.37.0";
  private static final X509Certificate[] NO_ISSUERS = {};
  private static final Set<CryptoPrimitive> SIGNATURE = EnumSet.of(CryptoPrimitive.SIGNATURE);

  private final BundleSet bundles;
  private final SpiffeIdMatcher peers;
  private final Clock clock;
  private final Observer observer;
  private final Map<TrustDomain, Set<TrustAnchor>> anchors;

  /**
   * What a handshake's verification of its peer came to, for metrics and logs. Called on the
   * handshake's thread, so it must be quick and must not throw.
   */
  public interface Observer {
    /** Counts nothing. */
    Observer NONE =
        new Observer() {
          @Override
          public void accepted(boolean server, SpiffeId id) {}

          @Override
          public void rejected(boolean server, PeerRejectedException rejection) {}
        };

    /**
     * A peer accepted.
     *
     * @param server whether the peer is a server (otherwise a client)
     */
    void accepted(boolean server, SpiffeId id);

    /**
     * A peer refused.
     *
     * @param server whether the peer is a server (otherwise a client)
     */
    void rejected(boolean server, PeerRejectedException rejection);
  }

  /**
   * A trust manager over one snapshot's bundles.
   *
   * @param bundles the bundles peers are verified against
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time chains are validated at
   */
  public SpiffeTrustManager(BundleSet bundles, SpiffeIdMatcher peers, Clock clock) {
    this(bundles, peers, clock, Observer.NONE);
  }

  /**
   * A trust manager over one snapshot's bundles that tells {@code observer} what each handshake's
   * verification came to.
   *
   * @param bundles the bundles peers are verified against
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time chains are validated at
   */
  public SpiffeTrustManager(
      BundleSet bundles, SpiffeIdMatcher peers, Clock clock, Observer observer) {
    this.bundles = bundles;
    this.peers = peers;
    this.clock = clock;
    this.observer = observer;
    Map<TrustDomain, Set<TrustAnchor>> byDomain = new HashMap<>();
    for (X509Bundle bundle : bundles.bundles()) {
      byDomain.put(bundle.trustDomain(), anchorsOf(bundle));
    }
    this.anchors = Map.copyOf(byDomain);
  }

  /**
   * Verifies a peer's chain for one purpose, and returns its SPIFFE ID.
   *
   * @param server whether the peer is a server (otherwise a client)
   * @throws PeerRejectedException saying why the peer is refused
   */
  public SpiffeId verify(X509Certificate[] chain, boolean server) throws PeerRejectedException {
    return verify(chain, server, null);
  }

  /**
   * Verifies a peer's chain for one purpose under a connection's algorithm constraints, and returns
   * its SPIFFE ID.
   *
   * @param server whether the peer is a server (otherwise a client)
   * @param constraints the connection's {@link SSLParameters#getAlgorithmConstraints()}, or null
   * @throws PeerRejectedException saying why the peer is refused
   */
  public SpiffeId verify(X509Certificate[] chain, boolean server, AlgorithmConstraints constraints)
      throws PeerRejectedException {
    if (chain == null || chain.length == 0) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.MALFORMED_SVID, "the peer presented no certificate");
    }
    X509Certificate leaf = chain[0];
    SpiffeId id;
    try {
      id = SvidLeaf.check(leaf);
    } catch (CertificateException e) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.MALFORMED_SVID,
          "the peer's SVID is invalid: " + e.getMessage(),
          e);
    }
    X509Bundle bundle =
        bundles
            .get(id.trustDomain())
            .orElseThrow(
                () ->
                    new PeerRejectedException(
                        PeerRejectedException.Reason.UNKNOWN_TRUST_DOMAIN,
                        "no bundle of the trust domain of " + id));
    TrustAnchor anchor = validate(id, chain, bundle);
    if (constraints != null) {
      permitted(id, chain, anchor, constraints);
    }
    purpose(id, leaf, server);
    if (!peers.matches(id)) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.ID_NOT_ALLOWED, "the peer " + id + " is not allowed");
    }
    return id;
  }

  /** Validates the chain against the bundle, and returns the authority it ends at. */
  private TrustAnchor validate(SpiffeId id, X509Certificate[] chain, X509Bundle bundle)
      throws PeerRejectedException {
    Set<TrustAnchor> trustAnchors = anchors.get(bundle.trustDomain());
    if (trustAnchors.isEmpty()) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.CHAIN,
          "the bundle of " + bundle.trustDomain() + " is empty");
    }
    List<X509Certificate> path = new ArrayList<>(List.of(chain));
    // A peer may send its root too; the path a validator takes ends below the anchor.
    while (path.size() > 1 && bundle.authorities().contains(path.get(path.size() - 1))) {
      path.remove(path.size() - 1);
    }
    try {
      CertPath certPath = CertificateFactory.getInstance("X.509").generateCertPath(path);
      PKIXParameters parameters = new PKIXParameters(trustAnchors);
      parameters.setRevocationEnabled(false);
      parameters.setDate(Date.from(clock.instant()));
      return ((PKIXCertPathValidatorResult)
              CertPathValidator.getInstance("PKIX").validate(certPath, parameters))
          .getTrustAnchor();
    } catch (GeneralSecurityException e) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.CHAIN,
          "the chain of "
              + id
              + " does not verify against the bundle of "
              + bundle.trustDomain()
              + ": "
              + e.getMessage(),
          e);
    }
  }

  /**
   * Applies a connection's algorithm constraints to the chain, as the JDK's own trust manager does:
   * the JDK does not add these checks around an {@link X509ExtendedTrustManager}, and PKIX applies
   * only {@code jdk.certpath.disabledAlgorithms}.
   */
  private static void permitted(
      SpiffeId id, X509Certificate[] chain, TrustAnchor anchor, AlgorithmConstraints constraints)
      throws PeerRejectedException {
    for (X509Certificate certificate : chain) {
      if (!constraints.permits(SIGNATURE, certificate.getPublicKey())) {
        throw new PeerRejectedException(
            PeerRejectedException.Reason.CHAIN,
            "the chain of "
                + id
                + " has a "
                + certificate.getPublicKey().getAlgorithm()
                + " key the connection's algorithm constraints forbid");
      }
      String algorithm = certificate.getSigAlgName();
      if (!constraints.permits(SIGNATURE, algorithm, parameters(certificate))) {
        throw new PeerRejectedException(
            PeerRejectedException.Reason.CHAIN,
            "the chain of "
                + id
                + " is signed with "
                + algorithm
                + ", which the connection's algorithm constraints forbid");
      }
    }
    if (!constraints.permits(SIGNATURE, anchor.getTrustedCert().getPublicKey())) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.CHAIN,
          "the authority of " + id + " has a key the connection's algorithm constraints forbid");
    }
  }

  /** A certificate's signature parameters (RSASSA-PSS has some), or null. */
  private static AlgorithmParameters parameters(X509Certificate certificate) {
    byte[] encoded = certificate.getSigAlgParams();
    if (encoded == null) {
      return null;
    }
    try {
      AlgorithmParameters parameters = AlgorithmParameters.getInstance(certificate.getSigAlgName());
      parameters.init(encoded);
      return parameters;
    } catch (GeneralSecurityException | IOException e) {
      return null;
    }
  }

  private static AlgorithmConstraints constraints(SSLEngine engine) {
    return engine == null ? null : engine.getSSLParameters().getAlgorithmConstraints();
  }

  private static AlgorithmConstraints constraints(Socket socket) {
    return socket instanceof SSLSocket ssl
        ? ssl.getSSLParameters().getAlgorithmConstraints()
        : null;
  }

  private static void purpose(SpiffeId id, X509Certificate leaf, boolean server)
      throws PeerRejectedException {
    List<String> usages;
    try {
      usages = leaf.getExtendedKeyUsage();
    } catch (CertificateException e) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.MALFORMED_SVID,
          "the extended key usage of " + id + " cannot be read",
          e);
    }
    String wanted = server ? SERVER_AUTH : CLIENT_AUTH;
    if (usages != null && !usages.contains(wanted) && !usages.contains(ANY_EXTENDED_KEY_USAGE)) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.CHAIN,
          "the SVID of " + id + " is not for TLS " + (server ? "servers" : "clients"));
    }
  }

  private static Set<TrustAnchor> anchorsOf(X509Bundle bundle) {
    Set<TrustAnchor> set = new HashSet<>();
    for (X509Certificate authority : bundle.authorities()) {
      set.add(new TrustAnchor(authority, null));
    }
    return Set.copyOf(set);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    observed(chain, false, null);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    observed(chain, false, constraints(socket));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    observed(chain, false, constraints(engine));
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    observed(chain, true, null);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    observed(chain, true, constraints(socket));
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    observed(chain, true, constraints(engine));
  }

  /** A handshake's verification, told to the observer. */
  private void observed(X509Certificate[] chain, boolean server, AlgorithmConstraints constraints)
      throws PeerRejectedException {
    SpiffeId id;
    try {
      id = verify(chain, server, constraints);
    } catch (PeerRejectedException e) {
      observer.rejected(server, e);
      throw e;
    }
    observer.accepted(server, id);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return NO_ISSUERS.clone();
  }
}
