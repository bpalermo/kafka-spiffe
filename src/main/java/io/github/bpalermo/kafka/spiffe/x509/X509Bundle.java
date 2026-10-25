package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * The X.509 authorities of one trust domain: what verifies that trust domain's SVIDs, and no
 * other's.
 *
 * @param trustDomain the trust domain the authorities belong to
 * @param authorities its root certificates; {@link SvidValidator} refuses an empty bundle
 *     <p>Public API (design, section 3).
 */
public record X509Bundle(TrustDomain trustDomain, List<X509Certificate> authorities) {

  public X509Bundle {
    if (trustDomain == null) {
      throw new IllegalArgumentException("a bundle belongs to a trust domain");
    }
    authorities = List.copyOf(authorities);
  }

  @Override
  public String toString() {
    return "X509Bundle[" + trustDomain + ", " + authorities.size() + " authorities]";
  }
}
