package io.github.bpalermo.kafka.spiffe.x509;

import java.security.cert.CertificateException;

/**
 * A peer's certificate chain that {@link SpiffeTrustManager} refused, and the kind of reason.
 *
 * <p>Public API (design, section 3).
 */
public final class PeerRejectedException extends CertificateException {
  private static final long serialVersionUID = 1L;

  /** Why a peer was refused, coarse enough to count. */
  public enum Reason {
    /** No chain, or a leaf that is not a valid X.509-SVID. */
    MALFORMED_SVID,
    /** No bundle of the trust domain the peer's SPIFFE ID names. */
    UNKNOWN_TRUST_DOMAIN,
    /** The chain does not verify against that trust domain's bundle, or not for this purpose. */
    CHAIN,
    /** A valid SVID whose SPIFFE ID the allow-list does not cover. */
    ID_NOT_ALLOWED,
  }

  private final Reason reason;

  public PeerRejectedException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public PeerRejectedException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }
}
