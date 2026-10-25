package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.workload.X509MaterialSource;
import io.github.bpalermo.kafka.spiffe.workload.X509SourceRegistry;
import io.github.bpalermo.kafka.spiffe.x509.SnapshotSslContext;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.lang.ref.Cleaner;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka's {@code ssl.engine.factory.class} for SPIFFE (design, sections 2 and 4): every engine
 * comes from the newest snapshot of the Workload API, so a rotation takes effect on the next
 * handshake, with no rebuild and no file, and connections already open keep what they negotiated.
 *
 * <p>Peers are verified by SPIFFE ID against the {@code spiffe.peer.allowed.*} allow-list and the
 * bundle of their own trust domain, never by host name. {@link #keystore()} and {@link
 * #truststore()} are null, so Kafka's keystore checks and validating handshake on reconfiguration
 * do not apply. {@code spiffe.*} keys are not reconfigurable: a factory Kafka rebuilds keeps its
 * predecessor's.
 *
 * <p>Kafka does not close every factory it makes: {@code SslFactory.validateReconfiguration} builds
 * one and drops it, and so does a failed validation handshake (3.0 to 4.4). So a factory registers
 * no listener on the shared source, which would keep it reachable, and releases its handle on the
 * source when it is closed or, failing that, collected.
 *
 * <p>Public API (design, section 3).
 */
public final class SpiffeSslEngineFactory implements SslEngineFactory {

  /** Kafka's ssl.client.auth, whose constant is in an internal package. */
  static final String SSL_CLIENT_AUTH = "ssl.client.auth";

  /** The keys whose change rebuilds the factory. */
  static final Set<String> RECONFIGURABLE =
      Set.of(
          SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
          SslConfigs.SSL_CIPHER_SUITES_CONFIG,
          SSL_CLIENT_AUTH);

  /** Keys that point at key or trust material: set, they suggest a mistake. */
  private static final List<String> MATERIAL_KEYS =
      List.of(
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          "ssl.keystore.key",
          "ssl.keystore.certificate.chain",
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          "ssl.truststore.certificates");

  private static final Logger LOG = LoggerFactory.getLogger(SpiffeSslEngineFactory.class);
  private static final AtomicBoolean ENDPOINT_IDENTIFICATION_LOGGED = new AtomicBoolean();
  private static final Cleaner CLEANER = Cleaner.create();

  private final Function<String, X509SourceRegistry> registries;
  private final Clock clock;
  private final Map<String, String> environment;

  private Map<String, ?> configs;
  private SpiffeConfig config;
  private SnapshotSslContext.Settings tls;
  private List<String> enabledProtocols;
  private List<String> cipherSuites;
  private String clientAuth;
  private X509MaterialSource source;
  private PeerVerification verification;
  private Cleaner.Cleanable release;
  private volatile Snapshot snapshot;

  /** A snapshot and the JDK context made of it. */
  private record Snapshot(X509Context context, SSLContext ssl) {}

  /** The factory Kafka instantiates: the process's shared sources, the system clock. */
  public SpiffeSslEngineFactory() {
    this(X509SourceRegistry::shared, Clock.systemUTC(), System.getenv());
  }

  SpiffeSslEngineFactory(
      Function<String, X509SourceRegistry> registries,
      Clock clock,
      Map<String, String> environment) {
    this.registries = registries;
    this.clock = clock;
    this.environment = environment;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = Map.copyOf(withoutNulls(configs));
    config = SpiffeConfig.parse(configs, environment);
    String protocol = string(configs, SslConfigs.SSL_PROTOCOL_CONFIG);
    tls =
        new SnapshotSslContext.Settings(
            protocol == null || protocol.isEmpty() ? SslConfigs.DEFAULT_SSL_PROTOCOL : protocol,
            emptyToNull(string(configs, SslConfigs.SSL_PROVIDER_CONFIG)));
    enabledProtocols = list(configs, SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
    cipherSuites = list(configs, SslConfigs.SSL_CIPHER_SUITES_CONFIG);
    clientAuth = emptyToNull(string(configs, SSL_CLIENT_AUTH));
    // Only a broker's configuration has ssl.client.auth; a broker verifies its clients.
    if (clientAuth != null
        && !"required".equalsIgnoreCase(clientAuth)
        && !config.allowUnauthenticated()) {
      throw new ConfigException(
          SSL_CLIENT_AUTH,
          clientAuth,
          "a broker using SPIFFE requires client certificates; set it to required, or "
              + SpiffeConfig.SERVER_ALLOW_UNAUTHENTICATED
              + "=true");
    }
    warnAboutIgnoredKeys(configs);

    PeerVerification counted = PeerVerification.acquire(config.jmxDomain());
    X509MaterialSource acquired;
    try {
      acquired = registries.apply(config.jmxDomain()).acquire(config.source());
    } catch (RuntimeException e) {
      counted.release();
      throw e;
    }
    verification = counted;
    source = acquired;
    // The cleaning action holds the handle and the counts, never this factory.
    release =
        CLEANER.register(
            this,
            () -> {
              acquired.close();
              counted.release();
            });
    try {
      source.awaitFirst(config.startupTimeout());
      rebuild(source.current());
    } catch (TimeoutException e) {
      close();
      throw new KafkaException(e.getMessage(), e);
    } catch (InterruptedException e) {
      close();
      Thread.currentThread().interrupt();
      throw new KafkaException("interrupted while waiting for the first SVID", e);
    } catch (RuntimeException e) {
      close();
      throw e;
    }
    if (snapshot == null) {
      close();
      throw new KafkaException(
          "no usable SVID from the Workload API at " + config.source().socket());
    }
    LOG.info(
        "SPIFFE engine factory for {} ready: peers {}, SVID {}",
        config.source().socket(),
        config.peers(),
        snapshot.context().svid().id());
  }

  @Override
  public SSLEngine createClientSslEngine(
      String peerHost, int peerPort, String endpointIdentification) {
    SSLEngine engine = current().ssl().createSSLEngine(peerHost, peerPort);
    engine.setUseClientMode(true);
    parameters(engine);
    return engine;
  }

  @Override
  public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
    SSLEngine engine = current().ssl().createSSLEngine(peerHost, peerPort);
    engine.setUseClientMode(false);
    if (clientAuth == null || "required".equalsIgnoreCase(clientAuth)) {
      engine.setNeedClientAuth(true);
    } else if ("requested".equalsIgnoreCase(clientAuth)) {
      engine.setWantClientAuth(true);
    }
    parameters(engine);
    return engine;
  }

  @Override
  public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
    for (String key : RECONFIGURABLE) {
      if (!Objects.equals(
          normalised(key, configs.get(key)), normalised(key, nextConfigs.get(key)))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return RECONFIGURABLE;
  }

  /** Null: there is no key store, and Kafka's keystore checks do not apply to SVIDs. */
  @Override
  public KeyStore keystore() {
    return null;
  }

  /** Null: trust comes from the Workload API's bundles. */
  @Override
  public KeyStore truststore() {
    return null;
  }

  @Override
  public void close() {
    source = null;
    Cleaner.Cleanable held = release;
    release = null;
    if (held != null) {
      held.clean();
    }
  }

  /** The snapshot to make an engine from, refused once expired under spiffe.svid.expired=fail. */
  private Snapshot current() {
    Snapshot current = snapshot;
    X509MaterialSource held = source;
    if (held != null) {
      // Reading the source also logs a late rotation or an expired SVID (design, section 6).
      X509Context latest = held.current();
      if (current == null || latest != current.context()) {
        rebuild(latest);
        current = snapshot;
      }
    }
    if (current == null) {
      throw new KafkaException("the SPIFFE engine factory is not configured");
    }
    if (config.expiredSvid() == SpiffeConfig.ExpiredSvid.FAIL
        && !clock.instant().isBefore(current.context().expiresAt())) {
      throw new KafkaException(
          "SVID "
              + current.context().svid().id()
              + " expired at "
              + current.context().expiresAt()
              + " and the Workload API at "
              + config.source().socket()
              + " has not replaced it ("
              + SpiffeConfig.SVID_EXPIRED
              + "=fail)");
    }
    return current;
  }

  /**
   * Builds the JDK context of the source's newest snapshot; a failure keeps the one in use.
   *
   * <p>{@code offered} is what the caller saw, which may be stale by the time the lock is taken: a
   * caller that read one rotation and paused while other threads installed it and the next must not
   * roll the factory back. So the snapshot built is the source's newest, read under the lock, and
   * {@code offered} is used only once the source is gone.
   */
  private synchronized void rebuild(X509Context offered) {
    X509MaterialSource held = source;
    X509Context context = held == null ? offered : held.current();
    Snapshot current = snapshot;
    if (current != null && current.context() == context) {
      return;
    }
    try {
      snapshot =
          new Snapshot(
              context,
              SnapshotSslContext.create(context, config.peers(), tls, clock, verification));
    } catch (GeneralSecurityException e) {
      if (current == null) {
        throw new KafkaException("cannot make a TLS context of the SVID: " + e.getMessage(), e);
      }
      LOG.error(
          "Cannot make a TLS context of the new SVID {}; keeping the current one",
          context.svid().id(),
          e);
    }
  }

  private void parameters(SSLEngine engine) {
    SSLParameters parameters = engine.getSSLParameters();
    // Peers are verified by SPIFFE ID, never by host name, whatever Kafka asked for.
    parameters.setEndpointIdentificationAlgorithm(null);
    // An empty list means the JDK's defaults, as in Kafka's own DefaultSslEngineFactory (3.0 to
    // 4.4), so that this factory replaces it without changing what an empty setting means.
    if (!enabledProtocols.isEmpty()) {
      parameters.setProtocols(enabledProtocols.toArray(String[]::new));
    }
    if (!cipherSuites.isEmpty()) {
      parameters.setCipherSuites(cipherSuites.toArray(String[]::new));
    }
    engine.setSSLParameters(parameters);
  }

  private static void warnAboutIgnoredKeys(Map<String, ?> configs) {
    String identification =
        string(configs, SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    if (identification != null
        && !identification.isEmpty()
        && ENDPOINT_IDENTIFICATION_LOGGED.compareAndSet(false, true)) {
      LOG.info(
          "{}={} is ignored: peers are verified by SPIFFE ID, never by host name",
          SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
          identification);
    }
    for (String key : MATERIAL_KEYS) {
      Object value = configs.get(key);
      if (value != null && !value.toString().isEmpty()) {
        LOG.warn(
            "{} is set and ignored: the SPIFFE engine factory takes its key and trust from the"
                + " Workload API",
            key);
      }
    }
  }

  private static String string(Map<String, ?> configs, String key) {
    Object value = configs.get(key);
    return value == null ? null : value.toString().trim();
  }

  private static String emptyToNull(String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  /** A list setting, given parsed (a List) or as Kafka's comma-separated string. */
  static List<String> list(Map<String, ?> configs, String key) {
    Object value = configs.get(key);
    if (value == null) {
      return List.of();
    }
    if (value instanceof List<?> items) {
      return items.stream()
          .map(Object::toString)
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .toList();
    }
    return Arrays.stream(value.toString().split(",", -1))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  /** A reconfigurable value, comparable however Kafka spelled it: a list, or a lowercase word. */
  private static Object normalised(String key, Object value) {
    if (SSL_CLIENT_AUTH.equals(key)) {
      String text = value == null ? "" : value.toString().trim();
      return text.isEmpty() ? null : text.toLowerCase(java.util.Locale.ROOT);
    }
    return value == null ? List.of() : list(Map.of(key, value), key);
  }

  private static Map<String, ?> withoutNulls(Map<String, ?> configs) {
    Map<String, Object> copy = new java.util.HashMap<>();
    configs.forEach(
        (key, value) -> {
          if (value != null) {
            copy.put(key, value);
          }
        });
    return copy;
  }
}
