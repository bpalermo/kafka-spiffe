package io.github.bpalermo.kafka.spiffe.testing;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Throwaway SPIFFE authorities for tests: roots, intermediates and X.509-SVIDs, including the
 * malformed ones a verifier must refuse. Certificates are issued with BouncyCastle, since the JDK
 * has no public API that issues one, around a fixed instant that tests set their clocks to.
 */
public final class TestPki {

  /** The instant certificates are issued around. */
  public static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  private static final AtomicLong SERIAL = new AtomicLong(1);

  private TestPki() {}

  /** A root authority of {@code trustDomain}. */
  public static Authority root(String trustDomain) {
    KeyPair keys = ec();
    X500Name name = new X500Name("O=SPIFFE,CN=" + trustDomain + " root");
    X509v3CertificateBuilder builder =
        builder(name, name, keys, NOW.minus(Duration.ofDays(1)), NOW.plus(Duration.ofDays(365)));
    authorityExtensions(builder, trustDomain);
    return new Authority(trustDomain, keys, sign(builder, keys.getPrivate()), List.of());
  }

  /** An X.509-SVID and its key, as the Workload API delivers them: leaf, then intermediates. */
  public record Issued(List<X509Certificate> chain, PrivateKey key) {
    public X509Certificate leaf() {
      return chain.get(0);
    }
  }

  /** A root, or an intermediate with the intermediates above it. */
  public static final class Authority {
    private final String trustDomain;
    private final KeyPair keys;
    private final X509Certificate certificate;

    /** This authority and the intermediates above it, up to and excluding the root. */
    private final List<X509Certificate> path;

    private Authority(
        String trustDomain,
        KeyPair keys,
        X509Certificate certificate,
        List<X509Certificate> above) {
      this.trustDomain = trustDomain;
      this.keys = keys;
      this.certificate = certificate;
      this.path = above;
    }

    /** The authority's own certificate: for a root, what a bundle holds. */
    public X509Certificate certificate() {
      return certificate;
    }

    /** An intermediate authority under this one, of the same trust domain. */
    public Authority intermediate() {
      KeyPair intermediateKeys = ec();
      X509v3CertificateBuilder builder =
          builder(
              subject(certificate),
              new X500Name("O=SPIFFE,CN=" + trustDomain + " intermediate " + SERIAL.get()),
              intermediateKeys,
              NOW.minus(Duration.ofDays(1)),
              NOW.plus(Duration.ofDays(30)));
      authorityExtensions(builder, trustDomain);
      X509Certificate intermediate = sign(builder, keys.getPrivate());
      List<X509Certificate> above = new ArrayList<>();
      above.add(intermediate);
      above.addAll(path);
      return new Authority(trustDomain, intermediateKeys, intermediate, List.copyOf(above));
    }

    /** An SVID for {@code path} of this authority's trust domain, valid an hour around NOW. */
    public Issued svid(String idPath) {
      return leaf().uris("spiffe://" + trustDomain + idPath).issue();
    }

    /** A leaf to customise; by default it is a valid SVID, but has no URI name until given one. */
    public LeafBuilder leaf() {
      return new LeafBuilder(this);
    }
  }

  /** The parts of a leaf certificate a test may get wrong on purpose. */
  public static final class LeafBuilder {
    private final Authority issuer;
    private final List<String> uris = new ArrayList<>();
    private Instant notBefore = NOW.minus(Duration.ofMinutes(30));
    private Instant notAfter = NOW.plus(Duration.ofMinutes(30));
    private boolean ca;
    private Integer keyUsage =
        KeyUsage.digitalSignature | KeyUsage.keyEncipherment | KeyUsage.keyAgreement;
    private List<KeyPurposeId> purposes =
        List.of(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth);
    private boolean rsa;

    private LeafBuilder(Authority issuer) {
      this.issuer = issuer;
    }

    public LeafBuilder uris(String... values) {
      uris.addAll(List.of(values));
      return this;
    }

    public LeafBuilder validity(Instant from, Instant to) {
      notBefore = from;
      notAfter = to;
      return this;
    }

    public LeafBuilder ca() {
      ca = true;
      return this;
    }

    /** The key usage bits ({@link KeyUsage} constants), or null to leave the extension out. */
    public LeafBuilder keyUsage(Integer usage) {
      keyUsage = usage;
      return this;
    }

    /** The extended key usages; none leaves the extension out. */
    public LeafBuilder purposes(KeyPurposeId... values) {
      purposes = List.of(values);
      return this;
    }

    public LeafBuilder rsa() {
      rsa = true;
      return this;
    }

    public Issued issue() {
      KeyPair keys = rsa ? rsaKeys() : ec();
      X509v3CertificateBuilder builder =
          builder(subject(issuer.certificate), new X500Name("O=SPIFFE"), keys, notBefore, notAfter);
      if (!uris.isEmpty()) {
        extension(
            builder,
            Extension.subjectAlternativeName,
            false,
            new GeneralNames(
                uris.stream()
                    .map(u -> new GeneralName(GeneralName.uniformResourceIdentifier, u))
                    .toArray(GeneralName[]::new)));
      }
      extension(builder, Extension.basicConstraints, true, new BasicConstraints(ca));
      if (keyUsage != null) {
        extension(builder, Extension.keyUsage, true, new KeyUsage(keyUsage));
      }
      if (!purposes.isEmpty()) {
        extension(
            builder,
            Extension.extendedKeyUsage,
            false,
            new ExtendedKeyUsage(purposes.toArray(KeyPurposeId[]::new)));
      }
      List<X509Certificate> chain = new ArrayList<>();
      chain.add(sign(builder, issuer.keys.getPrivate()));
      chain.addAll(issuer.path);
      return new Issued(List.copyOf(chain), keys.getPrivate());
    }
  }

  private static void authorityExtensions(X509v3CertificateBuilder builder, String trustDomain) {
    extension(builder, Extension.basicConstraints, true, new BasicConstraints(true));
    extension(
        builder, Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    extension(
        builder,
        Extension.subjectAlternativeName,
        false,
        new GeneralNames(
            new GeneralName(GeneralName.uniformResourceIdentifier, "spiffe://" + trustDomain)));
  }

  private static void extension(
      X509v3CertificateBuilder builder,
      ASN1ObjectIdentifier oid,
      boolean critical,
      ASN1Encodable value) {
    try {
      builder.addExtension(oid, critical, value);
    } catch (CertIOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static X500Name subject(X509Certificate certificate) {
    return X500Name.getInstance(certificate.getSubjectX500Principal().getEncoded());
  }

  private static X509v3CertificateBuilder builder(
      X500Name issuer, X500Name subject, KeyPair keys, Instant from, Instant to) {
    return new JcaX509v3CertificateBuilder(
        issuer,
        BigInteger.valueOf(SERIAL.getAndIncrement()),
        Date.from(from),
        Date.from(to),
        subject,
        keys.getPublic());
  }

  private static X509Certificate sign(X509v3CertificateBuilder builder, PrivateKey key) {
    String algorithm = "RSA".equals(key.getAlgorithm()) ? "SHA256withRSA" : "SHA256withECDSA";
    try {
      return new JcaX509CertificateConverter()
          .getCertificate(builder.build(new JcaContentSignerBuilder(algorithm).build(key)));
    } catch (GeneralSecurityException | OperatorCreationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static KeyPair ec() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
      generator.initialize(new ECGenParameterSpec("secp256r1"));
      return generator.generateKeyPair();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static KeyPair rsaKeys() {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      return generator.generateKeyPair();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}
