package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.x509.SvidLeaf;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka's {@code principal.builder.class} for SPIFFE (design, section 5): a TLS peer's principal is
 * the SPIFFE ID of its verified leaf, by default {@code Spiffe:spiffe://example.org/...}, or {@code
 * <type>:spiffe-unauthorized} for a peer the principal allow-list does not cover. Plaintext
 * connections get {@code User:ANONYMOUS} and SASL ones Kafka's own principal, as with Kafka's
 * default builder.
 *
 * <p>Kafka makes a builder per connection and calls {@link #build} for every request, on the
 * network thread. {@link #configure} reuses the policy parsed for the same configuration, and a
 * principal is computed once per TLS session or SASL server. The builder never touches the Workload
 * API.
 *
 * <p>It declares {@link KafkaPrincipalSerde} itself: from Kafka 4.2 {@link KafkaPrincipalBuilder}
 * extends it, and up to 4.1 a builder that does not declare it gets no serde, so requests forwarded
 * to the controller fail.
 */
public final class SpiffePrincipalBuilder
    implements KafkaPrincipalBuilder, KafkaPrincipalSerde, Configurable {

  /** Kafka's sasl.kerberos.principal.to.local.rules, for SASL/GSSAPI principals. */
  static final String KERBEROS_RULES = "sasl.kerberos.principal.to.local.rules";

  /** How many configurations' policies are kept: a broker has one, tests a few. */
  private static final int POLICIES = 16;

  private static final Map<Map<String, Object>, PrincipalPolicy> CACHE =
      new LinkedHashMap<>(POLICIES, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<Map<String, Object>, PrincipalPolicy> e) {
          return size() > POLICIES;
        }
      };

  /** Kafka's serde, which writes the type and the name; the builder makes no subclass. */
  private static final DefaultKafkaPrincipalBuilder SERDE =
      new DefaultKafkaPrincipalBuilder(null, null);

  private static final Logger LOG = LoggerFactory.getLogger(SpiffePrincipalBuilder.class);
  private static final LogLimiter REFUSALS = new LogLimiter(10, 60_000);

  private final Map<String, String> environment;
  private PrincipalPolicy policy;
  private Map<String, ?> configs;
  private DefaultKafkaPrincipalBuilder sasl;
  private volatile Built last;

  /** The principal of one session, or of one SASL server. */
  private record Built(Object key, KafkaPrincipal principal) {}

  /** The builder Kafka instantiates. */
  public SpiffePrincipalBuilder() {
    this(System.getenv());
  }

  SpiffePrincipalBuilder(Map<String, String> environment) {
    this.environment = environment;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    Map<String, Object> key = relevant(configs);
    synchronized (CACHE) {
      PrincipalPolicy cached = CACHE.get(key);
      if (cached != null) {
        policy = cached;
        return;
      }
    }
    PrincipalPolicy parsed = PrincipalPolicy.of(SpiffeConfig.parse(configs, environment), configs);
    synchronized (CACHE) {
      policy = CACHE.computeIfAbsent(key, k -> parsed);
    }
  }

  @Override
  public KafkaPrincipal build(AuthenticationContext context) {
    if (policy == null) {
      throw new IllegalStateException("the SPIFFE principal builder is not configured");
    }
    if (context instanceof PlaintextAuthenticationContext) {
      return KafkaPrincipal.ANONYMOUS;
    }
    if (context instanceof SslAuthenticationContext ssl) {
      Built built = last;
      if (built != null && built.key() == ssl.session()) {
        return built.principal();
      }
      KafkaPrincipal principal = tls(ssl);
      last = new Built(ssl.session(), principal);
      return principal;
    }
    if (context instanceof SaslAuthenticationContext saslContext) {
      Built built = last;
      if (built != null && built.key() == saslContext.server()) {
        return built.principal();
      }
      KafkaPrincipal principal = sasl().build(context);
      last = new Built(saslContext.server(), principal);
      return principal;
    }
    throw new IllegalArgumentException(
        "unhandled authentication context: " + context.getClass().getName());
  }

  /** The policy in use: one per configuration, shared by the builders made with it. */
  PrincipalPolicy policy() {
    return policy;
  }

  @Override
  public byte[] serialize(KafkaPrincipal principal) throws SerializationException {
    return SERDE.serialize(principal);
  }

  @Override
  public KafkaPrincipal deserialize(byte[] bytes) throws SerializationException {
    return SERDE.deserialize(bytes);
  }

  private KafkaPrincipal tls(SslAuthenticationContext context) {
    SpiffeId id;
    try {
      id = peer(context.session());
    } catch (SSLPeerUnverifiedException e) {
      return refused(context, "the peer sent no certificate");
    } catch (CertificateException e) {
      return refused(context, "its certificate holds no SPIFFE ID: " + e.getMessage());
    }
    PrincipalPolicy.Outcome outcome = policy.principal(id, context.listenerName());
    if (outcome.refusal() != null) {
      logRefusal(context, id.toString(), outcome.refusal());
    }
    return outcome.principal();
  }

  /** The SPIFFE ID of the verified leaf, parsed again: nothing is taken from the handshake. */
  private static SpiffeId peer(SSLSession session)
      throws SSLPeerUnverifiedException, CertificateException {
    Certificate[] chain = session.getPeerCertificates();
    if (chain.length == 0 || !(chain[0] instanceof X509Certificate leaf)) {
      throw new CertificateException("the peer's certificate is not an X.509 certificate");
    }
    return SvidLeaf.spiffeId(leaf);
  }

  private KafkaPrincipal refused(SslAuthenticationContext context, String reason) {
    logRefusal(context, "a peer", reason);
    return policy.refused(reason).principal();
  }

  private void logRefusal(AuthenticationContext context, String who, String reason) {
    long suppressed = REFUSALS.acquire();
    if (suppressed >= 0) {
      LOG.warn(
          "{} on listener {} from {} gets {}: not in {}{}",
          who,
          context.listenerName(),
          context.clientAddress(),
          policy.unauthorized(),
          reason,
          suppressed > 0 ? " (" + suppressed + " refusals not logged before this one)" : "");
    }
  }

  /** Kafka's own builder, for SASL connections, made with Kafka's Kerberos rules. */
  private DefaultKafkaPrincipalBuilder sasl() {
    if (sasl == null) {
      Object rules = configs.get(KERBEROS_RULES);
      @SuppressWarnings("unchecked")
      List<String> parsed =
          rules == null
              ? List.of("DEFAULT")
              : (List<String>) ConfigDef.parseType(KERBEROS_RULES, rules, ConfigDef.Type.LIST);
      sasl =
          new DefaultKafkaPrincipalBuilder(
              KerberosShortNamer.fromUnparsedRules(defaultRealm(), parsed), null);
    }
    return sasl;
  }

  /** The default Kerberos realm, as Kafka's SASL channel builder finds it; empty if none. */
  private static String defaultRealm() {
    try {
      return new KerberosPrincipal("tmp", KerberosPrincipal.KRB_NT_PRINCIPAL).getRealm();
    } catch (IllegalArgumentException e) {
      return "";
    }
  }

  /** The keys a policy is made of, so equal configurations share it. */
  private static Map<String, Object> relevant(Map<String, ?> configs) {
    Map<String, Object> key = new TreeMap<>();
    configs.forEach(
        (name, value) -> {
          if (name.startsWith(SpiffeConfig.PREFIX)
              || name.contains("." + SpiffeConfig.PREFIX)
              || name.equals(PrincipalPolicy.SSL_PRINCIPAL_MAPPING_RULES)
              || name.equals(PrincipalPolicy.ALLOW_EVERYONE_IF_NO_ACL_FOUND)) {
            key.put(name, value == null ? null : value.toString());
          }
        });
    return key;
  }
}
