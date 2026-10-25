package io.github.bpalermo.kafka.spiffe.workload;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.x509.X509Bundle;
import io.github.bpalermo.kafka.spiffe.x509.X509Svid;
import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One {@code X509SVIDResponse} of the Workload API, decoded: the SVIDs it carries, each with its
 * own trust domain's bundle, and the federated bundles.
 *
 * <pre>
 * message X509SVIDResponse { repeated X509SVID svids = 1; repeated bytes crl = 2;
 *                            map&lt;string, bytes&gt; federated_bundles = 3; }
 * message X509SVID { string spiffe_id = 1; bytes x509_svid = 2; bytes x509_svid_key = 3;
 *                    bytes bundle = 4; string hint = 5; }
 * </pre>
 *
 * <p>Certificates are ASN.1 DER, concatenated; the key is PKCS#8 DER. Revocation lists are ignored.
 * Fields this version does not know are skipped, so a newer agent is still understood.
 *
 * @param svids the SVIDs, in the agent's order; the first is the default
 * @param bundles the bundles of the SVIDs' trust domains and the federated ones, by trust domain
 */
record X509SvidResponse(List<X509Svid> svids, Map<TrustDomain, X509Bundle> bundles) {

  X509SvidResponse {
    svids = List.copyOf(svids);
    bundles = Map.copyOf(bundles);
  }

  /**
   * Decodes a response.
   *
   * @throws InvalidResponseException if it is not a well-formed response
   */
  static X509SvidResponse parse(byte[] message) throws InvalidResponseException {
    try {
      List<X509Svid> svids = new ArrayList<>();
      Map<TrustDomain, X509Bundle> bundles = new LinkedHashMap<>();
      Map<TrustDomain, X509Bundle> federated = new LinkedHashMap<>();
      WireReader reader = new WireReader(message);
      while (reader.hasNext()) {
        int tag = reader.tag();
        int field = tag >>> 3;
        int wireType = tag & 7;
        if (field == 1 && wireType == WireReader.LENGTH_DELIMITED) {
          svid(reader.message(), svids, bundles);
        } else if (field == 3 && wireType == WireReader.LENGTH_DELIMITED) {
          federatedBundle(reader.message(), federated);
        } else {
          reader.skip(wireType);
        }
      }
      // A federated bundle never replaces the one delivered with an SVID of that trust domain.
      federated.forEach(bundles::putIfAbsent);
      return new X509SvidResponse(svids, bundles);
    } catch (WireReader.WireException | IllegalArgumentException e) {
      throw new InvalidResponseException("a malformed X509SVIDResponse: " + e.getMessage(), e);
    }
  }

  private static void svid(
      WireReader reader, List<X509Svid> svids, Map<TrustDomain, X509Bundle> bundles)
      throws WireReader.WireException, InvalidResponseException {
    String id = "";
    byte[] chain = null;
    byte[] key = null;
    byte[] bundle = null;
    String hint = "";
    while (reader.hasNext()) {
      int tag = reader.tag();
      int wireType = tag & 7;
      switch (tag >>> 3) {
        case 1 -> id = expectBytes(reader, wireType) ? reader.string() : id;
        case 2 -> chain = expectBytes(reader, wireType) ? reader.bytes() : chain;
        case 3 -> key = expectBytes(reader, wireType) ? reader.bytes() : key;
        case 4 -> bundle = expectBytes(reader, wireType) ? reader.bytes() : bundle;
        case 5 -> hint = expectBytes(reader, wireType) ? reader.string() : hint;
        default -> reader.skip(wireType);
      }
    }
    if (chain == null || key == null || bundle == null) {
      throw new InvalidResponseException("an SVID of " + id + " without its chain, key or bundle");
    }
    SpiffeId spiffeId = SpiffeId.parse(id);
    X509Svid svid;
    try {
      svid = X509Svid.of(certificates(chain), privateKey(key), hint);
    } catch (CertificateException e) {
      throw new InvalidResponseException("the SVID of " + id + ": " + e.getMessage(), e);
    }
    if (!svid.id().equals(spiffeId)) {
      throw new InvalidResponseException(
          "the SVID delivered as " + id + " names " + svid.id() + " in its certificate");
    }
    svids.add(svid);
    TrustDomain domain = spiffeId.trustDomain();
    bundles.putIfAbsent(domain, new X509Bundle(domain, certificates(bundle)));
  }

  private static void federatedBundle(WireReader reader, Map<TrustDomain, X509Bundle> federated)
      throws WireReader.WireException, InvalidResponseException {
    String key = null;
    byte[] value = null;
    while (reader.hasNext()) {
      int tag = reader.tag();
      int wireType = tag & 7;
      switch (tag >>> 3) {
        case 1 -> key = expectBytes(reader, wireType) ? reader.string() : key;
        case 2 -> value = expectBytes(reader, wireType) ? reader.bytes() : value;
        default -> reader.skip(wireType);
      }
    }
    if (key == null || value == null) {
      throw new InvalidResponseException("a federated bundle without its trust domain or bundle");
    }
    // The agent keys federated bundles by trust domain ID (spiffe://example.org); accept the name
    // too. A SPIFFE ID with a path names a workload, not a trust domain, and is refused.
    TrustDomain domain;
    if (key.startsWith(SpiffeId.SCHEME)) {
      SpiffeId id = SpiffeId.parse(key);
      if (!id.isTrustDomainId()) {
        throw new InvalidResponseException(
            "a federated bundle keyed by " + key + ", which is not a trust domain ID");
      }
      domain = id.trustDomain();
    } else {
      domain = TrustDomain.parse(key);
    }
    if (federated.put(domain, new X509Bundle(domain, certificates(value))) != null) {
      throw new InvalidResponseException("two federated bundles of " + domain);
    }
  }

  /**
   * Whether the field is length-delimited, as every field read here is; otherwise it is skipped.
   */
  private static boolean expectBytes(WireReader reader, int wireType)
      throws WireReader.WireException {
    if (wireType == WireReader.LENGTH_DELIMITED) {
      return true;
    }
    reader.skip(wireType);
    return false;
  }

  private static List<X509Certificate> certificates(byte[] der) throws InvalidResponseException {
    try {
      List<X509Certificate> certificates = new ArrayList<>();
      for (var certificate :
          CertificateFactory.getInstance("X.509")
              .generateCertificates(new ByteArrayInputStream(der))) {
        certificates.add((X509Certificate) certificate);
      }
      if (certificates.isEmpty()) {
        throw new InvalidResponseException("no certificate where one was expected");
      }
      return certificates;
    } catch (CertificateException e) {
      throw new InvalidResponseException(
          "certificates that cannot be parsed: " + e.getMessage(), e);
    }
  }

  private static PrivateKey privateKey(byte[] pkcs8) throws InvalidResponseException {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(pkcs8);
    for (String algorithm : new String[] {"EC", "RSA", "Ed25519", "Ed448"}) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (GeneralSecurityException e) {
        // Not this algorithm; try the next.
      }
    }
    throw new InvalidResponseException("a private key that is not PKCS#8 EC, RSA or EdDSA");
  }

  /** A response that cannot be decoded; the snapshot in use stays. */
  static final class InvalidResponseException extends Exception {
    private static final long serialVersionUID = 1L;

    InvalidResponseException(String message) {
      super(message);
    }

    InvalidResponseException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
