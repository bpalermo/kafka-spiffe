package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What principal a verified SPIFFE ID gets on a listener (design, sections 4 and 5): the listener's
 * allow-list ({@code spiffe.principal.listener.<listener>.*}, else {@code
 * spiffe.principal.allowed.*}, else the peer allow-list), then the mapping rules, under the
 * configured type. A peer outside the allow-list, or that no rule maps, gets {@code
 * <type>:spiffe-unauthorized}.
 */
final class PrincipalPolicy {

  /** The name of the principal a peer that gets none is given. */
  static final String UNAUTHORIZED = "spiffe-unauthorized";

  /** Kafka's ssl.principal.mapping.rules, used only by its own builder. */
  static final String SSL_PRINCIPAL_MAPPING_RULES = "ssl.principal.mapping.rules";

  /** Kafka's allow.everyone.if.no.acl.found, read by its authorizers. */
  static final String ALLOW_EVERYONE_IF_NO_ACL_FOUND = "allow.everyone.if.no.acl.found";

  private static final Pattern LISTENER_KEY =
      Pattern.compile(
          Pattern.quote(SpiffeConfig.PRINCIPAL_LISTENER_PREFIX)
              + "([^.]+)\\.allowed\\.(ids|trust\\.domains|id\\.patterns)");

  /** A type is split from its name at the first colon, so it cannot hold one. */
  private static final Pattern TYPE = Pattern.compile("[^:\\s]+");

  /** How many distinct principal names are remembered for the trust domain warning. */
  private static final int WARNED_NAMES = 1024;

  private static final Logger LOG = LoggerFactory.getLogger(PrincipalPolicy.class);

  private final String type;
  private final SslPrincipalMapper mapper;
  private final boolean identityMapping;
  private final SpiffeIdMatcher allowed;
  private final Map<String, SpiffeIdMatcher> listeners;
  private final KafkaPrincipal unauthorized;
  private final Set<String> warnedNames = ConcurrentHashMap.newKeySet();

  /** A principal, or the reason a peer gets none. */
  record Outcome(KafkaPrincipal principal, String refusal) {}

  private PrincipalPolicy(
      String type, String rules, SpiffeIdMatcher allowed, Map<String, SpiffeIdMatcher> listeners) {
    this.type = type;
    this.identityMapping = "DEFAULT".equals(rules.trim());
    try {
      this.mapper = SslPrincipalMapper.fromRules(rules);
    } catch (IllegalArgumentException e) {
      ConfigException failure =
          new ConfigException(SpiffeConfig.PRINCIPAL_MAPPING_RULES, rules, e.getMessage());
      failure.initCause(e);
      throw failure;
    }
    this.allowed = allowed;
    this.listeners = Map.copyOf(listeners);
    this.unauthorized = new KafkaPrincipal(type, UNAUTHORIZED);
  }

  /**
   * The policy of a parsed configuration; {@code configs} is the whole map, for the Kafka keys it
   * warns about.
   *
   * @throws ConfigException naming the first key that is wrong
   */
  static PrincipalPolicy of(SpiffeConfig config, Map<String, ?> configs) {
    String type = ((String) config.value(SpiffeConfig.PRINCIPAL_TYPE)).trim();
    if (!TYPE.matcher(type).matches()) {
      throw new ConfigException(
          SpiffeConfig.PRINCIPAL_TYPE, type, "a principal type is a word with no colon");
    }
    String rules = (String) config.value(SpiffeConfig.PRINCIPAL_MAPPING_RULES);
    SpiffeIdMatcher allowed =
        set(config, SpiffeConfig.PRINCIPAL_ALLOWED_IDS)
                || set(config, SpiffeConfig.PRINCIPAL_ALLOWED_TRUST_DOMAINS)
                || set(config, SpiffeConfig.PRINCIPAL_ALLOWED_ID_PATTERNS)
            ? matcher(
                SpiffeConfig.PRINCIPAL_ALLOWED_IDS,
                list(config, SpiffeConfig.PRINCIPAL_ALLOWED_IDS),
                list(config, SpiffeConfig.PRINCIPAL_ALLOWED_TRUST_DOMAINS),
                list(config, SpiffeConfig.PRINCIPAL_ALLOWED_ID_PATTERNS))
            : config.peers();
    PrincipalPolicy policy = new PrincipalPolicy(type, rules, allowed, listeners(config));
    policy.warn(configs);
    return policy;
  }

  /** The principal {@code id} gets on {@code listener} (any case; null when Kafka gives none). */
  Outcome principal(SpiffeId id, String listener) {
    SpiffeIdMatcher matcher = matcher(listener);
    if (!matcher.matches(id)) {
      return refused(
          (listener != null && listeners.containsKey(listener.toLowerCase(Locale.ROOT))
                  ? "the allow-list of listener " + listener
                  : "the principal allow-list")
              + " "
              + matcher);
    }
    String name;
    try {
      name = mapper.getName(id.toString());
    } catch (IOException e) {
      return refused("no mapping rule matches it (" + e.getMessage() + ")");
    }
    if (name == null || name.isEmpty()) {
      return refused("its mapping rule gives an empty name");
    }
    if (!identityMapping
        && !name.toLowerCase(Locale.ROOT).contains(id.trustDomain().name())
        && warnedNames.size() < WARNED_NAMES
        && warnedNames.add(name)) {
      LOG.warn(
          "The mapping rules give {} the principal name {}, which does not name its trust domain:"
              + " another trust domain's IDs could get the same principal",
          id,
          name);
    }
    return new Outcome(new KafkaPrincipal(type, name), null);
  }

  /** The principal of a peer that gets none. */
  KafkaPrincipal unauthorized() {
    return unauthorized;
  }

  /** A peer that gets no principal, for a reason. */
  Outcome refused(String reason) {
    return new Outcome(unauthorized, reason);
  }

  private SpiffeIdMatcher matcher(String listener) {
    if (listener != null) {
      SpiffeIdMatcher own = listeners.get(listener.toLowerCase(Locale.ROOT));
      if (own != null) {
        return own;
      }
    }
    return allowed;
  }

  private void warn(Map<String, ?> configs) {
    Object kafkaRules = configs.get(SSL_PRINCIPAL_MAPPING_RULES);
    if (kafkaRules != null && !"DEFAULT".equals(kafkaRules.toString().trim())) {
      LOG.warn(
          "{} is used only by Kafka's own principal builder and has no effect on SPIFFE"
              + " principals; their rules are {}",
          SSL_PRINCIPAL_MAPPING_RULES,
          SpiffeConfig.PRINCIPAL_MAPPING_RULES);
    }
    Object allowEveryone = configs.get(ALLOW_EVERYONE_IF_NO_ACL_FOUND);
    if (allowEveryone != null && Boolean.parseBoolean(allowEveryone.toString().trim())) {
      LOG.warn(
          "{}=true lets {} through wherever a resource has no ACL: a peer outside the principal"
              + " allow-list is kept out by ACLs alone",
          ALLOW_EVERYONE_IF_NO_ACL_FOUND,
          unauthorized);
    }
  }

  /** The per-listener allow-lists, by lowercased listener name. */
  private static Map<String, SpiffeIdMatcher> listeners(SpiffeConfig config) {
    Map<String, Map<String, Object>> byListener = new TreeMap<>();
    Map<String, String> spelled = new HashMap<>();
    config
        .principalListenerKeys()
        .forEach(
            (key, value) -> {
              Matcher m = LISTENER_KEY.matcher(key);
              if (!m.matches()) {
                throw new ConfigException(key, value, "an unknown spiffe.* key");
              }
              String listener = m.group(1).toLowerCase(Locale.ROOT);
              String previous = spelled.putIfAbsent(listener, m.group(1));
              if (previous != null && !previous.equals(m.group(1))) {
                throw new ConfigException(
                    key,
                    value,
                    "listener "
                        + previous
                        + " is also spelled "
                        + m.group(1)
                        + "; listener names are not case-sensitive here");
              }
              byListener.computeIfAbsent(listener, l -> new HashMap<>()).put(m.group(2), value);
            });
    Map<String, SpiffeIdMatcher> listeners = new HashMap<>();
    byListener.forEach(
        (listener, keys) -> {
          String prefix =
              SpiffeConfig.PRINCIPAL_LISTENER_PREFIX + spelled.get(listener) + ".allowed.";
          listeners.put(
              listener,
              matcher(
                  prefix + "*",
                  parseList(keys.get("ids")),
                  parseList(keys.get("trust.domains")),
                  parseList(keys.get("id.patterns"))));
        });
    return listeners;
  }

  private static SpiffeIdMatcher matcher(
      String key, List<String> ids, List<String> domains, List<String> patterns) {
    try {
      return SpiffeIdMatcher.none().withIds(ids).withTrustDomains(domains).withPatterns(patterns);
    } catch (IllegalArgumentException e) {
      ConfigException failure = new ConfigException(key + ": " + e.getMessage());
      failure.initCause(e);
      throw failure;
    }
  }

  private static boolean set(SpiffeConfig config, String key) {
    return config.value(key) != null;
  }

  private static List<String> list(SpiffeConfig config, String key) {
    @SuppressWarnings("unchecked")
    List<String> value = (List<String>) config.value(key);
    return value == null ? List.of() : value;
  }

  @SuppressWarnings("unchecked")
  private static List<String> parseList(Object value) {
    if (value == null) {
      return List.of();
    }
    return (List<String>) ConfigDef.parseType("list", value, ConfigDef.Type.LIST);
  }
}
