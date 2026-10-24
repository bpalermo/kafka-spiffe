package io.github.bpalermo.kafka.spiffe.x509;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BundleSetTest {

  private static final TrustDomain OWN = TrustDomain.parse("example.org");
  private static final TrustDomain FEDERATED = TrustDomain.parse("k8s.example.org");

  private static X509Bundle bundle(TrustDomain domain) {
    return new X509Bundle(domain, List.of(TestPki.root(domain.name()).certificate()));
  }

  @Test
  void findsABundleByTrustDomain() {
    X509Bundle own = bundle(OWN);
    BundleSet set = BundleSet.of(List.of(own, bundle(FEDERATED)));
    assertEquals(own, set.get(OWN).orElseThrow());
    assertTrue(set.get(TrustDomain.parse("other.example.org")).isEmpty());
    assertEquals(Set.of(OWN, FEDERATED), set.trustDomains());
  }

  @Test
  void refusesTwoBundlesOfOneTrustDomain() {
    assertThrows(
        IllegalArgumentException.class, () -> BundleSet.of(List.of(bundle(OWN), bundle(OWN))));
  }

  @Test
  void keepsOnlyTheTrustDomainsNamed() {
    BundleSet set = BundleSet.of(List.of(bundle(OWN), bundle(FEDERATED)));
    BundleSet kept = set.retainOnly(Set.of(OWN, TrustDomain.parse("absent.example.org")));
    assertEquals(Set.of(OWN), kept.trustDomains());
  }
}
