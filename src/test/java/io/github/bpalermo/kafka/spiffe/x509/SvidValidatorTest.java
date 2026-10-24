package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class SvidValidatorTest {

  private static final TestPki.Authority ROOT = TestPki.root("example.org");
  private static final TestPki.Authority FEDERATED = TestPki.root("k8s.example.org");

  private static X509Svid svid(TestPki.Issued issued) throws CertificateException {
    return X509Svid.of(issued.chain(), issued.key(), "");
  }

  private static X509Bundle bundle(TestPki.Authority authority, String trustDomain) {
    return new X509Bundle(TrustDomain.parse(trustDomain), List.of(authority.certificate()));
  }

  private static X509Context context(TestPki.Issued issued, X509Bundle... bundles)
      throws CertificateException {
    return new X509Context(svid(issued), BundleSet.of(List.of(bundles)));
  }

  @Test
  void acceptsAValidSnapshot() throws CertificateException {
    X509Context context =
        context(
            ROOT.intermediate().svid("/broker"),
            bundle(ROOT, "example.org"),
            bundle(FEDERATED, "k8s.example.org"));
    assertDoesNotThrow(() -> SvidValidator.validate(context, TestPki.NOW));
  }

  @Test
  void acceptsAnRsaSvid() throws CertificateException {
    X509Svid svid = svid(ROOT.leaf().uris("spiffe://example.org/a").rsa().issue());
    assertDoesNotThrow(() -> SvidValidator.validate(svid, TestPki.NOW));
  }

  @Test
  void refusesAnSvidThatIsNotValidNow() throws CertificateException {
    X509Svid svid = svid(ROOT.svid("/a"));
    InvalidSvidException expired =
        assertThrows(
            InvalidSvidException.class,
            () -> SvidValidator.validate(svid, TestPki.NOW.plus(Duration.ofHours(1))));
    assertTrue(expired.getMessage().contains("not valid at"), expired.getMessage());
    assertThrows(
        InvalidSvidException.class,
        () -> SvidValidator.validate(svid, TestPki.NOW.minus(Duration.ofHours(1))));
  }

  @Test
  void refusesAKeyThatIsNotTheLeafs() throws CertificateException {
    TestPki.Issued one = ROOT.svid("/a");
    TestPki.Issued other = ROOT.svid("/a");
    X509Svid mixed = X509Svid.of(one.chain(), other.key(), "");
    InvalidSvidException e =
        assertThrows(InvalidSvidException.class, () -> SvidValidator.validate(mixed, TestPki.NOW));
    assertTrue(e.getMessage().contains("private key"), e.getMessage());
  }

  @Test
  void refusesAKeyOfAnotherAlgorithm() throws CertificateException {
    TestPki.Issued ec = ROOT.svid("/a");
    TestPki.Issued rsa = ROOT.leaf().uris("spiffe://example.org/a").rsa().issue();
    X509Svid mixed = X509Svid.of(ec.chain(), rsa.key(), "");
    assertThrows(InvalidSvidException.class, () -> SvidValidator.validate(mixed, TestPki.NOW));
  }

  @Test
  void refusesALeafThatIsNotAnSvid() throws CertificateException {
    X509Svid ca = svid(ROOT.leaf().uris("spiffe://example.org/a").ca().issue());
    assertThrows(InvalidSvidException.class, () -> SvidValidator.validate(ca, TestPki.NOW));
  }

  @Test
  void refusesASnapshotWithoutItsOwnTrustDomainsBundle() throws CertificateException {
    X509Context context = context(ROOT.svid("/a"), bundle(FEDERATED, "k8s.example.org"));
    InvalidSvidException e =
        assertThrows(
            InvalidSvidException.class, () -> SvidValidator.validate(context, TestPki.NOW));
    assertTrue(e.getMessage().contains("own trust domain"), e.getMessage());
  }

  @Test
  void refusesAnEmptyBundle() throws CertificateException {
    X509Context context =
        context(
            ROOT.svid("/a"),
            bundle(ROOT, "example.org"),
            new X509Bundle(TrustDomain.parse("k8s.example.org"), List.of()));
    InvalidSvidException e =
        assertThrows(
            InvalidSvidException.class, () -> SvidValidator.validate(context, TestPki.NOW));
    assertEquals("the bundle of k8s.example.org is empty", e.getMessage());
  }

  @Test
  void anSvidReadsItsIdFromTheLeafAndHidesItsKey() throws CertificateException {
    TestPki.Issued issued = ROOT.svid("/a");
    X509Svid svid = X509Svid.of(issued.chain(), issued.key(), "internal");
    assertEquals("spiffe://example.org/a", svid.id().toString());
    assertTrue(svid.toString().contains("hint internal"), svid.toString());
    assertTrue(!svid.toString().contains(issued.key().toString()), svid.toString());
  }

  @Test
  void anSvidNeedsAChainWithAValidId() {
    assertThrows(
        CertificateException.class, () -> X509Svid.of(List.of(), ROOT.svid("/a").key(), ""));
    TestPki.Issued noId = ROOT.leaf().issue();
    assertThrows(CertificateException.class, () -> X509Svid.of(noId.chain(), noId.key(), ""));
  }
}
