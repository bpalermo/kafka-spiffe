package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The checks the X.509-SVID specification asks of a leaf certificate, whoever presents it: this
 * workload's own SVID, or a peer's.
 *
 * <ul>
 *   <li>exactly one URI subject alternative name, a SPIFFE ID in canonical form ({@link
 *       SpiffeId#parse}) that is not a trust domain's own ID;
 *   <li>not a certificate authority (no {@code cA} basic constraint);
 *   <li>a key usage extension with {@code digitalSignature}, and neither {@code keyCertSign} nor
 *       {@code cRLSign}.
 * </ul>
 *
 * <p>The subject DN, its common name and any DNS or IP name are never looked at.
 */
public final class SvidLeaf {

  private static final int URI_NAME = 6;
  private static final int DIGITAL_SIGNATURE = 0;
  private static final int KEY_CERT_SIGN = 5;
  private static final int CRL_SIGN = 6;

  private SvidLeaf() {}

  /**
   * The SPIFFE ID of a leaf certificate that passes the checks.
   *
   * @throws CertificateException naming the first check it fails
   */
  public static SpiffeId check(X509Certificate leaf) throws CertificateException {
    SpiffeId id = spiffeId(leaf);
    if (leaf.getBasicConstraints() != -1) {
      throw new CertificateException("the leaf certificate of " + id + " is a CA certificate");
    }
    boolean[] usage = leaf.getKeyUsage();
    if (usage == null) {
      throw new CertificateException("the leaf certificate of " + id + " has no key usage");
    }
    if (!flag(usage, DIGITAL_SIGNATURE)) {
      throw new CertificateException(
          "the leaf certificate of " + id + " does not have the digitalSignature key usage");
    }
    if (flag(usage, KEY_CERT_SIGN) || flag(usage, CRL_SIGN)) {
      throw new CertificateException(
          "the leaf certificate of " + id + " has the keyCertSign or cRLSign key usage");
    }
    return id;
  }

  /**
   * The SPIFFE ID a certificate's one URI subject alternative name holds.
   *
   * @throws CertificateException if it has no URI name, several, or one that is not a canonical
   *     SPIFFE ID of a workload
   */
  public static SpiffeId spiffeId(X509Certificate certificate) throws CertificateException {
    List<String> uris = uris(certificate);
    if (uris.size() != 1) {
      throw new CertificateException(
          "an X.509-SVID has exactly one URI subject alternative name, this certificate has "
              + uris.size());
    }
    SpiffeId id;
    try {
      id = SpiffeId.parse(uris.get(0));
    } catch (IllegalArgumentException e) {
      throw new CertificateException(e.getMessage(), e);
    }
    if (id.isTrustDomainId()) {
      throw new CertificateException(
          "an X.509-SVID names a workload, not the trust domain itself (" + id + ")");
    }
    return id;
  }

  private static List<String> uris(X509Certificate certificate) throws CertificateParsingException {
    Collection<List<?>> names = certificate.getSubjectAlternativeNames();
    List<String> uris = new ArrayList<>();
    if (names != null) {
      for (List<?> name : names) {
        if (name.get(0) instanceof Integer type && type == URI_NAME) {
          uris.add((String) name.get(1));
        }
      }
    }
    return uris;
  }

  private static boolean flag(boolean[] usage, int bit) {
    return bit < usage.length && usage[bit];
  }
}
