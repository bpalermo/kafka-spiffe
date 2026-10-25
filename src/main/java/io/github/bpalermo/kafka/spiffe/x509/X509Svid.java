package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;

/**
 * An X.509-SVID: a SPIFFE ID, the certificate chain that proves it (leaf first, then any
 * intermediates) and the leaf's private key. Immutable.
 *
 * <p>Constructing one parses the SPIFFE ID out of the leaf and nothing more; {@link SvidValidator}
 * decides whether it is usable.
 *
 * @param id the SPIFFE ID of the leaf
 * @param chain the leaf, then its intermediates, never empty
 * @param key the leaf's private key
 * @param hint the Workload API's hint for this SVID, empty when it gave none
 *     <p>Public API (design, section 3).
 */
public record X509Svid(SpiffeId id, List<X509Certificate> chain, PrivateKey key, String hint) {

  public X509Svid {
    chain = List.copyOf(chain);
    if (chain.isEmpty()) {
      throw new IllegalArgumentException("an X.509-SVID needs a certificate chain");
    }
    if (key == null) {
      throw new IllegalArgumentException("an X.509-SVID needs a private key");
    }
    hint = hint == null ? "" : hint;
  }

  /**
   * An SVID made of a chain and a key, its ID read from the leaf.
   *
   * @throws CertificateException if the leaf does not carry exactly one valid SPIFFE ID
   */
  public static X509Svid of(List<X509Certificate> chain, PrivateKey key, String hint)
      throws CertificateException {
    if (chain.isEmpty()) {
      throw new CertificateException("the certificate chain is empty");
    }
    return new X509Svid(SvidLeaf.spiffeId(chain.get(0)), chain, key, hint);
  }

  /** The leaf certificate. */
  public X509Certificate leaf() {
    return chain.get(0);
  }

  /** When the leaf certificate expires. */
  public Instant expiresAt() {
    return leaf().getNotAfter().toInstant();
  }

  /** Leaves the key out, so that an SVID can be logged. */
  @Override
  public String toString() {
    return "X509Svid["
        + id
        + ", expires "
        + expiresAt()
        + (hint.isEmpty() ? "" : ", hint " + hint)
        + "]";
  }
}
