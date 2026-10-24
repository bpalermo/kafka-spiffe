package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.junit.jupiter.api.Test;

class SvidLeafTest {

  private static final TestPki.Authority ROOT = TestPki.root("example.org");

  private static String refusal(X509Certificate leaf) {
    return assertThrows(CertificateException.class, () -> SvidLeaf.check(leaf)).getMessage();
  }

  @Test
  void acceptsAnSvid() throws CertificateException {
    assertEquals(
        SpiffeId.parse("spiffe://example.org/ns/a/sa/b"),
        SvidLeaf.check(ROOT.svid("/ns/a/sa/b").leaf()));
  }

  @Test
  void acceptsAnSvidWithoutExtendedKeyUsage() throws CertificateException {
    X509Certificate leaf = ROOT.leaf().uris("spiffe://example.org/a").purposes().issue().leaf();
    assertEquals(SpiffeId.parse("spiffe://example.org/a"), SvidLeaf.check(leaf));
  }

  @Test
  void refusesNoUriName() {
    assertTrue(refusal(ROOT.leaf().issue().leaf()).contains("has 0"));
  }

  @Test
  void refusesTwoUriNames() {
    X509Certificate leaf =
        ROOT.leaf().uris("spiffe://example.org/a", "spiffe://example.org/b").issue().leaf();
    assertTrue(refusal(leaf).contains("has 2"));
  }

  @Test
  void refusesAUriThatIsNotACanonicalSpiffeId() {
    for (String uri :
        new String[] {
          "https://example.org/a",
          "spiffe://Example.org/a",
          "spiffe://example.org//a",
          "spiffe://example.org/./a",
          "spiffe://example.org/a/"
        }) {
      assertThrows(
          CertificateException.class,
          () -> SvidLeaf.check(ROOT.leaf().uris(uri).issue().leaf()),
          uri);
    }
  }

  @Test
  void refusesATrustDomainId() {
    assertTrue(
        refusal(ROOT.leaf().uris("spiffe://example.org").issue().leaf()).contains("trust domain"));
  }

  @Test
  void refusesACaCertificate() {
    X509Certificate leaf = ROOT.leaf().uris("spiffe://example.org/a").ca().issue().leaf();
    assertTrue(refusal(leaf).contains("CA certificate"));
  }

  @Test
  void refusesNoKeyUsage() {
    X509Certificate leaf = ROOT.leaf().uris("spiffe://example.org/a").keyUsage(null).issue().leaf();
    assertTrue(refusal(leaf).contains("no key usage"));
  }

  @Test
  void refusesAKeyUsageWithoutDigitalSignature() {
    X509Certificate leaf =
        ROOT.leaf()
            .uris("spiffe://example.org/a")
            .keyUsage(KeyUsage.keyEncipherment)
            .issue()
            .leaf();
    assertTrue(refusal(leaf).contains("digitalSignature"));
  }

  @Test
  void refusesKeyCertSignAndCrlSign() {
    for (int usage : new int[] {KeyUsage.keyCertSign, KeyUsage.cRLSign}) {
      X509Certificate leaf =
          ROOT.leaf()
              .uris("spiffe://example.org/a")
              .keyUsage(KeyUsage.digitalSignature | usage)
              .issue()
              .leaf();
      assertTrue(refusal(leaf).contains("keyCertSign or cRLSign"));
    }
  }
}
