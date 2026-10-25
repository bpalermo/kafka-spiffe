package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.github.bpalermo.kafka.spiffe.x509.BundleSet;
import io.github.bpalermo.kafka.spiffe.x509.SnapshotSslContext;
import io.github.bpalermo.kafka.spiffe.x509.X509Bundle;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.github.bpalermo.kafka.spiffe.x509.X509Svid;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;

/**
 * TLS sessions as a broker sees them after a handshake in memory: an example.org broker, and
 * clients of k8s.example.org whose SVIDs the test chooses.
 */
final class TlsPeers {

  static final TestPki.Authority BROKER_CA = TestPki.root("example.org");
  static final TestPki.Authority CLIENT_CA = TestPki.root("k8s.example.org");

  private static final List<X509Bundle> BOTH =
      List.of(
          new X509Bundle(TrustDomain.parse("example.org"), List.of(BROKER_CA.certificate())),
          new X509Bundle(TrustDomain.parse("k8s.example.org"), List.of(CLIENT_CA.certificate())));
  private static final Clock CLOCK = Clock.fixed(TestPki.NOW, ZoneOffset.UTC);
  private static final TestPki.Issued BROKER = BROKER_CA.svid("/broker");

  private TlsPeers() {}

  /** The broker's session with a client presenting {@code path}'s SVID of k8s.example.org. */
  static SSLSession client(String path) throws GeneralSecurityException, SSLException {
    return Handshake.run(
            context(CLIENT_CA.svid(path), "spiffe://example.org/broker", null),
            context(BROKER, null, "k8s.example.org"))
        .server();
  }

  /** The broker's session with a client that sent no certificate. */
  static SSLSession anonymous() throws GeneralSecurityException, SSLException {
    SSLEngine client =
        context(CLIENT_CA.svid("/ns/a/sa/b"), "spiffe://example.org/broker", null)
            .createSSLEngine("", 0);
    client.setUseClientMode(true);
    SSLEngine server = context(BROKER, null, "k8s.example.org").createSSLEngine();
    server.setUseClientMode(false);
    return Handshake.run(client, server).server();
  }

  static SslAuthenticationContext on(String listener, SSLSession session) {
    return new SslAuthenticationContext(session, InetAddress.getLoopbackAddress(), listener);
  }

  private static SSLContext context(TestPki.Issued issued, String allowedId, String allowedDomain)
      throws GeneralSecurityException {
    SpiffeIdMatcher peers =
        allowedId != null
            ? SpiffeIdMatcher.none().withIds(List.of(allowedId))
            : SpiffeIdMatcher.none().withTrustDomains(List.of(allowedDomain));
    return SnapshotSslContext.create(
        new X509Context(X509Svid.of(issued.chain(), issued.key(), ""), BundleSet.of(BOTH)),
        peers,
        SnapshotSslContext.Settings.DEFAULT,
        CLOCK);
  }
}
