package io.github.bpalermo.kafka.spiffe.x509;

import java.time.Instant;

/**
 * One snapshot of what the Workload API delivered: the SVID this workload presents and the bundles
 * it verifies peers against. Immutable, so a handshake that starts from a snapshot never sees half
 * of a rotation; a rotation makes a new one.
 *
 * @param svid the SVID this workload presents
 * @param bundles the bundles of its own trust domain and of those federated with it
 *     <p>Public API (design, section 3).
 */
public record X509Context(X509Svid svid, BundleSet bundles) {

  public X509Context {
    if (svid == null || bundles == null) {
      throw new IllegalArgumentException("a snapshot needs an SVID and bundles");
    }
  }

  /** When the snapshot's SVID expires. */
  public Instant expiresAt() {
    return svid.expiresAt();
  }
}
