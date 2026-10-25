package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.workload.X509MaterialSource;
import io.github.bpalermo.kafka.spiffe.workload.X509Source;
import io.github.bpalermo.kafka.spiffe.workload.X509SourceRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

/**
 * Every {@code spiffe.*} key (design, section 4), read from the map Kafka passes to {@code
 * configure()} and validated: an unknown {@code spiffe.*} key, and a {@code
 * listener.name.<listener>.spiffe.*} key, are errors, so a typo or a form that cannot work does not
 * go unnoticed.
 *
 * <p>Public API (design, section 3).
 */
public final class SpiffeConfig {

  public static final String PREFIX = "spiffe.";

  public static final String ENDPOINT_SOCKET = "spiffe.endpoint.socket";
  public static final String SVID_HINT = "spiffe.svid.hint";
  public static final String SVID_ID = "spiffe.svid.id";
  public static final String STARTUP_TIMEOUT_MS = "spiffe.startup.timeout.ms";
  public static final String RECONNECT_BACKOFF_MS = "spiffe.reconnect.backoff.ms";
  public static final String RECONNECT_BACKOFF_MAX_MS = "spiffe.reconnect.backoff.max.ms";
  public static final String BUNDLE_TRUST_DOMAINS = "spiffe.bundle.trust.domains";
  public static final String SVID_EXPIRED = "spiffe.svid.expired";

  public static final String PEER_ALLOWED_IDS = "spiffe.peer.allowed.ids";
  public static final String PEER_ALLOWED_TRUST_DOMAINS = "spiffe.peer.allowed.trust.domains";
  public static final String PEER_ALLOWED_ID_PATTERNS = "spiffe.peer.allowed.id.patterns";
  public static final String SERVER_ALLOW_UNAUTHENTICATED = "spiffe.server.allow.unauthenticated";

  public static final String PRINCIPAL_TYPE = "spiffe.principal.type";
  public static final String PRINCIPAL_MAPPING_RULES = "spiffe.principal.mapping.rules";
  public static final String PRINCIPAL_ALLOWED_IDS = "spiffe.principal.allowed.ids";
  public static final String PRINCIPAL_ALLOWED_TRUST_DOMAINS =
      "spiffe.principal.allowed.trust.domains";
  public static final String PRINCIPAL_ALLOWED_ID_PATTERNS = "spiffe.principal.allowed.id.patterns";
  public static final String PRINCIPAL_LISTENER_PREFIX = "spiffe.principal.listener.";

  public static final String METRICS_JMX_ENABLED = "spiffe.metrics.jmx.enabled";
  public static final String METRICS_JMX_DOMAIN = "spiffe.metrics.jmx.domain";

  /** The environment variable SPIFFE names the Workload API socket with. */
  public static final String ENDPOINT_SOCKET_ENV = "SPIFFE_ENDPOINT_SOCKET";

  /** {@code spiffe.principal.listener.<listener>.allowed.(ids|trust.domains|id.patterns)}. */
  private static final Pattern PRINCIPAL_LISTENER_KEY =
      Pattern.compile(
          "spiffe\\.principal\\.listener\\.[^.]+\\.allowed\\.(ids|trust\\.domains|id\\.patterns)");

  private static final Pattern LISTENER_PREFIXED =
      Pattern.compile("listener\\.name\\.[^.]+\\.spiffe\\..*");

  /** What engines do once the SVID has expired and no rotation came (design, section 6). */
  public enum ExpiredSvid {
    /** Keep presenting it; peers refuse it. */
    PRESENT,
    /** Refuse to create engines, with a message that names the cause. */
    FAIL
  }

  static final ConfigDef DEFINITION =
      new ConfigDef()
          .define(
              ENDPOINT_SOCKET,
              Type.STRING,
              null,
              Importance.HIGH,
              "unix:///path of the Workload API; $" + ENDPOINT_SOCKET_ENV + " when unset")
          .define(SVID_HINT, Type.STRING, "", Importance.LOW, "Pick the SVID with this hint")
          .define(SVID_ID, Type.STRING, null, Importance.LOW, "Pick the SVID with this SPIFFE ID")
          .define(
              STARTUP_TIMEOUT_MS,
              Type.LONG,
              120_000L,
              ConfigDef.Range.atLeast(-1),
              Importance.MEDIUM,
              "How long configure() waits for the first SVID; -1 for ever, 0 not at all")
          .define(
              RECONNECT_BACKOFF_MS,
              Type.LONG,
              500L,
              ConfigDef.Range.atLeast(1),
              Importance.LOW,
              "The first reconnect delay")
          .define(
              RECONNECT_BACKOFF_MAX_MS,
              Type.LONG,
              30_000L,
              ConfigDef.Range.atLeast(1),
              Importance.LOW,
              "The longest reconnect delay")
          .define(
              BUNDLE_TRUST_DOMAINS,
              Type.LIST,
              "",
              Importance.LOW,
              "The trust domains whose bundles are trusted; all when empty")
          .define(
              SVID_EXPIRED,
              Type.STRING,
              "present",
              ConfigDef.ValidString.in("present", "fail"),
              Importance.LOW,
              "present or fail, once the SVID has expired")
          .define(PEER_ALLOWED_IDS, Type.LIST, "", Importance.HIGH, "Peers allowed by SPIFFE ID")
          .define(
              PEER_ALLOWED_TRUST_DOMAINS,
              Type.LIST,
              "",
              Importance.HIGH,
              "Trust domains whose every ID is an allowed peer")
          .define(
              PEER_ALLOWED_ID_PATTERNS,
              Type.LIST,
              "",
              Importance.HIGH,
              "Path patterns of allowed peers")
          .define(
              SERVER_ALLOW_UNAUTHENTICATED,
              Type.BOOLEAN,
              false,
              Importance.LOW,
              "Allow a broker's ssl.client.auth to be other than required")
          .define(PRINCIPAL_TYPE, Type.STRING, "Spiffe", Importance.MEDIUM, "The principal type")
          .define(
              PRINCIPAL_MAPPING_RULES,
              Type.STRING,
              "DEFAULT",
              Importance.LOW,
              "Rules from SPIFFE ID to principal name, in the syntax of"
                  + " ssl.principal.mapping.rules")
          .define(
              PRINCIPAL_ALLOWED_IDS,
              Type.LIST,
              null,
              Importance.MEDIUM,
              "Peers that get a principal; the peer allow-list when unset")
          .define(
              PRINCIPAL_ALLOWED_TRUST_DOMAINS,
              Type.LIST,
              null,
              Importance.MEDIUM,
              "Trust domains whose IDs get a principal")
          .define(
              PRINCIPAL_ALLOWED_ID_PATTERNS,
              Type.LIST,
              null,
              Importance.MEDIUM,
              "Path patterns of IDs that get a principal")
          .define(METRICS_JMX_ENABLED, Type.BOOLEAN, true, Importance.LOW, "Register JMX MBeans")
          .define(
              METRICS_JMX_DOMAIN,
              Type.STRING,
              "kafka.spiffe",
              Importance.LOW,
              "The JMX domain of the MBeans");

  private final Map<String, Object> values;
  private final Map<String, Object> principalListenerKeys;
  private final X509Source.Settings source;
  private final SpiffeIdMatcher peers;

  private SpiffeConfig(
      Map<String, Object> values,
      Map<String, Object> principalListenerKeys,
      X509Source.Settings source,
      SpiffeIdMatcher peers) {
    this.values = values;
    this.principalListenerKeys = principalListenerKeys;
    this.source = source;
    this.peers = peers;
  }

  /**
   * Reads and validates the {@code spiffe.*} keys of a Kafka configuration map.
   *
   * @param environment where {@value #ENDPOINT_SOCKET_ENV} is looked up
   * @throws ConfigException naming the first key that is wrong
   */
  public static SpiffeConfig parse(Map<String, ?> configs, Map<String, String> environment) {
    Map<String, Object> spiffe = new HashMap<>();
    Map<String, Object> listenerKeys = new HashMap<>();
    for (Map.Entry<String, ?> entry : configs.entrySet()) {
      String key = entry.getKey();
      if (LISTENER_PREFIXED.matcher(key).matches()) {
        throw new ConfigException(
            key,
            entry.getValue(),
            "listener.name.<listener>.spiffe.* keys do not exist: Kafka may drop them before the"
                + " library sees them. Per-listener principal keys are "
                + PRINCIPAL_LISTENER_PREFIX
                + "<listener>.*");
      }
      if (!key.startsWith(PREFIX)) {
        continue;
      }
      if (PRINCIPAL_LISTENER_KEY.matcher(key).matches()) {
        listenerKeys.put(key, entry.getValue() == null ? "" : entry.getValue());
      } else if (DEFINITION.names().contains(key)) {
        spiffe.put(key, entry.getValue());
      } else {
        throw new ConfigException(key, entry.getValue(), "an unknown spiffe.* key");
      }
    }
    Map<String, Object> values = DEFINITION.parse(spiffe);

    String socket = (String) values.get(ENDPOINT_SOCKET);
    if (socket == null || socket.isEmpty()) {
      socket = environment.get(ENDPOINT_SOCKET_ENV);
    }
    if (socket == null || socket.isEmpty()) {
      throw new ConfigException(
          ENDPOINT_SOCKET,
          null,
          "neither it nor $" + ENDPOINT_SOCKET_ENV + " names the Workload API");
    }
    Path socketPath = socketPath(socket);

    SpiffeId svidId = null;
    String svidIdValue = (String) values.get(SVID_ID);
    if (svidIdValue != null && !svidIdValue.isEmpty()) {
      svidId = parse(SVID_ID, svidIdValue, SpiffeId::parse);
    }
    Set<TrustDomain> bundles = new LinkedHashSet<>();
    for (String name : list(values, BUNDLE_TRUST_DOMAINS)) {
      bundles.add(parse(BUNDLE_TRUST_DOMAINS, name, TrustDomain::parse));
    }
    long backoff = (Long) values.get(RECONNECT_BACKOFF_MS);
    long maxBackoff = (Long) values.get(RECONNECT_BACKOFF_MAX_MS);
    if (maxBackoff < backoff) {
      throw new ConfigException(
          RECONNECT_BACKOFF_MAX_MS,
          maxBackoff,
          "below " + RECONNECT_BACKOFF_MS + " (" + backoff + ")");
    }
    X509Source.Settings source =
        new X509Source.Settings(
            socketPath,
            (String) values.get(SVID_HINT),
            svidId,
            bundles,
            Duration.ofMillis(backoff),
            Duration.ofMillis(maxBackoff));

    SpiffeIdMatcher peers =
        matcher(values, PEER_ALLOWED_IDS, PEER_ALLOWED_TRUST_DOMAINS, PEER_ALLOWED_ID_PATTERNS);
    if (peers.isEmpty()) {
      throw new ConfigException(
          PEER_ALLOWED_IDS,
          "",
          "one of "
              + PEER_ALLOWED_IDS
              + ", "
              + PEER_ALLOWED_TRUST_DOMAINS
              + " and "
              + PEER_ALLOWED_ID_PATTERNS
              + " is required: there is no implicit 'anything a bundle vouches for'");
    }
    return new SpiffeConfig(values, Map.copyOf(listenerKeys), source, peers);
  }

  /** How the Workload API source is set up. */
  public X509Source.Settings source() {
    return source;
  }

  /**
   * A handle on the Workload API source that an engine factory configured with these keys uses: the
   * same shared source, while a factory holds it, so an application that needs the same SVIDs and
   * bundles outside Kafka reads the same stream. Close the handle when done; the source stops when
   * its last handle is closed. It connects in the background: {@link X509MaterialSource#awaitFirst}
   * waits for its first snapshot.
   */
  public X509MaterialSource acquireSource() {
    return X509SourceRegistry.shared(jmxDomain()).acquire(source);
  }

  /** The SPIFFE IDs accepted of peers in the handshake. */
  public SpiffeIdMatcher peers() {
    return peers;
  }

  /** How long configure() waits for the first SVID; negative for ever. */
  public Duration startupTimeout() {
    return Duration.ofMillis((Long) values.get(STARTUP_TIMEOUT_MS));
  }

  public ExpiredSvid expiredSvid() {
    return ExpiredSvid.valueOf(((String) values.get(SVID_EXPIRED)).toUpperCase(Locale.ROOT));
  }

  public boolean allowUnauthenticated() {
    return (Boolean) values.get(SERVER_ALLOW_UNAUTHENTICATED);
  }

  /** The JMX domain of the MBeans, or null if they are off. */
  public String jmxDomain() {
    return (Boolean) values.get(METRICS_JMX_ENABLED)
        ? (String) values.get(METRICS_JMX_DOMAIN)
        : null;
  }

  /** A parsed value, by key: for the principal builder's keys. */
  public Object value(String key) {
    return values.get(key);
  }

  /**
   * The {@code spiffe.principal.listener.<listener>.*} keys as they were given: a string, or a list
   * when Kafka or the caller parsed it.
   */
  public Map<String, Object> principalListenerKeys() {
    return principalListenerKeys;
  }

  /**
   * {@code unix:///path} or {@code unix:/path}: an absolute path. Nothing else is a socket here.
   */
  static Path socketPath(String socket) {
    String path;
    if (socket.startsWith("unix://")) {
      path = socket.substring("unix://".length());
    } else if (socket.startsWith("unix:")) {
      path = socket.substring("unix:".length());
    } else {
      throw new ConfigException(
          ENDPOINT_SOCKET,
          socket,
          "only unix:///path is accepted: the Workload API is a Unix socket");
    }
    if (!path.startsWith("/")) {
      throw new ConfigException(ENDPOINT_SOCKET, socket, "the socket path must be absolute");
    }
    return Path.of(path);
  }

  static SpiffeIdMatcher matcher(
      Map<String, Object> values, String ids, String domains, String patterns) {
    try {
      return SpiffeIdMatcher.none()
          .withIds(list(values, ids))
          .withTrustDomains(list(values, domains))
          .withPatterns(list(values, patterns));
    } catch (IllegalArgumentException e) {
      throw withCause(
          new ConfigException(ids + " / " + domains + " / " + patterns + ": " + e.getMessage()), e);
    }
  }

  @SuppressWarnings("unchecked")
  static List<String> list(Map<String, Object> values, String key) {
    Object value = values.get(key);
    return value == null ? List.of() : (List<String>) value;
  }

  private static <T> T parse(
      String key, String value, java.util.function.Function<String, T> parser) {
    try {
      return parser.apply(value.trim());
    } catch (IllegalArgumentException e) {
      throw withCause(new ConfigException(key, value, e.getMessage()), e);
    }
  }

  /** Kafka 3.0's ConfigException takes no cause in its constructors. */
  private static ConfigException withCause(ConfigException exception, Throwable cause) {
    exception.initCause(cause);
    return exception;
  }
}
