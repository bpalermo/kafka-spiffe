package io.github.bpalermo.kafka.spiffe.x509;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/**
 * Decides whether a snapshot is usable before anything presents it: an update that fails is
 * refused, and the snapshot in use stays.
 *
 * <p>The SVID's leaf must pass {@link SvidLeaf#check}, name the SVID's ID, be valid at the time
 * given, and belong to the private key. The snapshot must hold a bundle of the SVID's own trust
 * domain, and no bundle may be empty. The SVID's chain is not verified against its own bundle:
 * peers do that, and a snapshot refused for it would leave the old one in use until it expires.
 */
public final class SvidValidator {

  private static final byte[] CHALLENGE = {
    'k', 'a', 'f', 'k', 'a', '-', 's', 'p', 'i', 'f', 'f', 'e'
  };

  private SvidValidator() {}

  /**
   * Checks a snapshot at {@code now}.
   *
   * @throws InvalidSvidException saying what is wrong with it
   */
  public static void validate(X509Context context, Instant now) throws InvalidSvidException {
    validate(context.svid(), now);
    Optional<X509Bundle> own = context.bundles().get(context.svid().id().trustDomain());
    if (own.isEmpty()) {
      throw new InvalidSvidException(
          "no bundle of the SVID's own trust domain " + context.svid().id().trustDomain());
    }
    for (X509Bundle bundle : context.bundles().bundles()) {
      if (bundle.authorities().isEmpty()) {
        throw new InvalidSvidException("the bundle of " + bundle.trustDomain() + " is empty");
      }
    }
  }

  /**
   * Checks an SVID at {@code now}.
   *
   * @throws InvalidSvidException saying what is wrong with it
   */
  public static void validate(X509Svid svid, Instant now) throws InvalidSvidException {
    X509Certificate leaf = svid.leaf();
    try {
      if (!SvidLeaf.check(leaf).equals(svid.id())) {
        throw new InvalidSvidException("the leaf certificate does not name " + svid.id());
      }
    } catch (CertificateException e) {
      throw new InvalidSvidException(e.getMessage(), e);
    }
    try {
      leaf.checkValidity(Date.from(now));
    } catch (CertificateException e) {
      throw new InvalidSvidException(
          "the SVID of "
              + svid.id()
              + " is not valid at "
              + now
              + " (valid from "
              + leaf.getNotBefore().toInstant()
              + " to "
              + leaf.getNotAfter().toInstant()
              + ")",
          e);
    }
    if (!belongs(svid.key(), leaf)) {
      throw new InvalidSvidException(
          "the private key does not belong to the leaf certificate of " + svid.id());
    }
  }

  /** Signs with the key and verifies with the certificate: true only for a pair. */
  private static boolean belongs(PrivateKey key, X509Certificate leaf) {
    String algorithm =
        switch (key.getAlgorithm()) {
          case "RSA" -> "SHA256withRSA";
          case "EC" -> "SHA256withECDSA";
          default -> key.getAlgorithm();
        };
    try {
      Signature signer = Signature.getInstance(algorithm);
      signer.initSign(key);
      signer.update(CHALLENGE);
      byte[] signature = signer.sign();
      Signature verifier = Signature.getInstance(algorithm);
      verifier.initVerify(leaf.getPublicKey());
      verifier.update(CHALLENGE);
      return verifier.verify(signature);
    } catch (GeneralSecurityException e) {
      return false;
    }
  }
}
