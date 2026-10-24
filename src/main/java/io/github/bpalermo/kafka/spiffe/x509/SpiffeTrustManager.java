package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
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
 * <p>Every check is made whatever the engine or socket asks for: endpoint identification is
 * ignored. The time is the clock's, so that tests can set it.
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

  private final BundleSet bundles;
  private final SpiffeIdMatcher peers;
  private final Clock clock;
  private final Map<TrustDomain, Set<TrustAnchor>> anchors;

  /**
   * A trust manager over one snapshot's bundles.
   *
   * @param bundles the bundles peers are verified against
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time chains are validated at
   */
  public SpiffeTrustManager(BundleSet bundles, SpiffeIdMatcher peers, Clock clock) {
    this.bundles = bundles;
    this.peers = peers;
    this.clock = clock;
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
    validate(id, chain, bundle);
    purpose(id, leaf, server);
    if (!peers.matches(id)) {
      throw new PeerRejectedException(
          PeerRejectedException.Reason.ID_NOT_ALLOWED, "the peer " + id + " is not allowed");
    }
    return id;
  }

  private void validate(SpiffeId id, X509Certificate[] chain, X509Bundle bundle)
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
      CertPathValidator.getInstance("PKIX").validate(certPath, parameters);
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
    verify(chain, false);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    verify(chain, false);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    verify(chain, false);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    verify(chain, true);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    verify(chain, true);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    verify(chain, true);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return NO_ISSUERS.clone();
  }
}
