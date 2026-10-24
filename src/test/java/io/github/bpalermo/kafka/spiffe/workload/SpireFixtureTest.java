package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import io.github.bpalermo.kafka.spiffe.x509.BundleSet;
import io.github.bpalermo.kafka.spiffe.x509.SnapshotSslContext;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import io.github.bpalermo.kafka.spiffe.x509.SvidLeaf;
import io.github.bpalermo.kafka.spiffe.x509.SvidValidator;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.github.bpalermo.kafka.spiffe.x509.X509Svid;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

/**
 * A response recorded from a real SPIRE agent (SpireE2eTest, see AGENTS.md): what SPIRE issues
 * passes every check, so the checks are not only right about the test PKI's certificates.
 */
class SpireFixtureTest {

  private static byte[] fixture() throws IOException {
    try (InputStream in =
        SpireFixtureTest.class.getResourceAsStream("testdata/spire-x509-svid-response.bin")) {
      if (in == null) {
        throw new IOException("the recorded SPIRE response is missing");
      }
      return in.readAllBytes();
    }
  }

  @Test
  void whatSpireIssuesPassesEveryCheck() throws Exception {
    X509SvidResponse response = X509SvidResponse.parse(fixture());
    X509Svid svid = response.svids().get(0);
    assertEquals(SpiffeId.parse("spiffe://example.org/workload"), svid.id());
    assertEquals(svid.id(), SvidLeaf.check(svid.leaf()));

    Instant during = svid.leaf().getNotBefore().toInstant().plusSeconds(5);
    X509Context snapshot = new X509Context(svid, BundleSet.of(response.bundles().values()));
    SvidValidator.validate(snapshot, during);

    Clock clock = Clock.fixed(during, ZoneOffset.UTC);
    SpiffeIdMatcher itself = SpiffeIdMatcher.none().withIds(List.of(svid.id().toString()));
    SpiffeTrustManager trust = new SpiffeTrustManager(snapshot.bundles(), itself, clock);
    X509Certificate[] chain = svid.chain().toArray(X509Certificate[]::new);
    assertEquals(svid.id(), trust.verify(chain, true));
    assertEquals(svid.id(), trust.verify(chain, false));

    SSLContext context =
        SnapshotSslContext.create(snapshot, itself, SnapshotSslContext.Settings.DEFAULT, clock);
    Handshake.Sessions sessions = Handshake.run(context, context);
    assertEquals(
        svid.id(), SvidLeaf.spiffeId((X509Certificate) sessions.server().getPeerCertificates()[0]));
    assertTrue(snapshot.bundles().trustDomains().contains(TrustDomain.parse("example.org")));
  }
}
