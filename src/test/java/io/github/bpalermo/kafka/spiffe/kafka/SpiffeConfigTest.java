package io.github.bpalermo.kafka.spiffe.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class SpiffeConfigTest {

  private static Map<String, Object> minimal() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("spiffe.endpoint.socket", "unix:///run/spire/agent.sock");
    configs.put("spiffe.peer.allowed.ids", "spiffe://example.org/broker");
    configs.put("ssl.protocol", "TLSv1.3");
    configs.put("bootstrap.servers", "b:9093");
    return configs;
  }

  private static SpiffeConfig parse(Map<String, Object> configs) {
    return SpiffeConfig.parse(configs, Map.of());
  }

  private static String refusal(Map<String, Object> configs) {
    return assertThrows(ConfigException.class, () -> parse(configs)).getMessage();
  }

  @Test
  void readsTheDefaults() {
    SpiffeConfig config = parse(minimal());
    assertEquals(Path.of("/run/spire/agent.sock"), config.source().socket());
    assertEquals("", config.source().svidHint());
    assertNull(config.source().svidId());
    assertEquals(Set.of(), config.source().bundleTrustDomains());
    assertEquals(Duration.ofMillis(500), config.source().backoff());
    assertEquals(Duration.ofSeconds(30), config.source().maxBackoff());
    assertEquals(Duration.ofMinutes(2), config.startupTimeout());
    assertEquals(SpiffeConfig.ExpiredSvid.PRESENT, config.expiredSvid());
    assertEquals("kafka.spiffe", config.jmxDomain());
    assertTrue(config.peers().matches(SpiffeId.parse("spiffe://example.org/broker")));
  }

  @Test
  void readsEverySourceKey() {
    Map<String, Object> configs = minimal();
    configs.put("spiffe.svid.hint", "internal");
    configs.put("spiffe.svid.id", "spiffe://example.org/me");
    configs.put("spiffe.bundle.trust.domains", "example.org, k8s.example.org");
    configs.put("spiffe.reconnect.backoff.ms", "100");
    configs.put("spiffe.reconnect.backoff.max.ms", 1000L);
    configs.put("spiffe.startup.timeout.ms", "-1");
    configs.put("spiffe.svid.expired", "fail");
    configs.put("spiffe.metrics.jmx.enabled", "false");
    SpiffeConfig config = parse(configs);
    assertEquals("internal", config.source().svidHint());
    assertEquals(SpiffeId.parse("spiffe://example.org/me"), config.source().svidId());
    assertEquals(
        Set.of(TrustDomain.parse("example.org"), TrustDomain.parse("k8s.example.org")),
        config.source().bundleTrustDomains());
    assertEquals(Duration.ofMillis(100), config.source().backoff());
    assertEquals(Duration.ofSeconds(1), config.source().maxBackoff());
    assertTrue(config.startupTimeout().isNegative());
    assertEquals(SpiffeConfig.ExpiredSvid.FAIL, config.expiredSvid());
    assertNull(config.jmxDomain());
  }

  @Test
  void takesTheSocketFromTheEnvironmentWhenUnset() {
    Map<String, Object> configs = minimal();
    configs.remove("spiffe.endpoint.socket");
    SpiffeConfig config =
        SpiffeConfig.parse(configs, Map.of("SPIFFE_ENDPOINT_SOCKET", "unix:/tmp/agent.sock"));
    assertEquals(Path.of("/tmp/agent.sock"), config.source().socket());
    assertTrue(refusal(configs).contains("SPIFFE_ENDPOINT_SOCKET"));
  }

  @Test
  void acceptsOnlyAbsoluteUnixSockets() {
    for (String socket :
        List.of(
            "tcp://127.0.0.1:8081", "/run/agent.sock", "unix:relative.sock", "unix://relative")) {
      Map<String, Object> configs = minimal();
      configs.put("spiffe.endpoint.socket", socket);
      assertThrows(ConfigException.class, () -> parse(configs), socket);
    }
  }

  @Test
  void refusesUnknownAndListenerPrefixedKeys() {
    Map<String, Object> typo = minimal();
    typo.put("spiffe.peer.allowed.idz", "x");
    assertTrue(refusal(typo).contains("unknown"));

    Map<String, Object> prefixed = minimal();
    prefixed.put("listener.name.mtls.spiffe.peer.allowed.ids", "spiffe://example.org/x");
    assertTrue(refusal(prefixed).contains("spiffe.principal.listener."));

    Map<String, Object> badListenerKey = minimal();
    badListenerKey.put("spiffe.principal.listener.mtls.allowed.nothing", "x");
    assertTrue(refusal(badListenerKey).contains("unknown"));
  }

  @Test
  void acceptsThePrincipalBuildersKeysIncludingPerListenerOnes() {
    Map<String, Object> configs = minimal();
    configs.put("spiffe.principal.type", "User");
    configs.put("spiffe.principal.mapping.rules", "RULE:^spiffe://(.*)$/$1/,DEFAULT");
    configs.put("spiffe.principal.allowed.trust.domains", "example.org");
    configs.put("spiffe.principal.listener.mtls.allowed.ids", "spiffe://example.org/a");
    configs.put("spiffe.principal.listener.internal.allowed.trust.domains", "example.org");
    configs.put("spiffe.principal.listener.internal.allowed.id.patterns", "spiffe://example.org/*");
    SpiffeConfig config = parse(configs);
    assertEquals("User", config.value("spiffe.principal.type"));
    assertEquals(3, config.principalListenerKeys().size());
  }

  @Test
  void requiresAPeerAllowList() {
    Map<String, Object> configs = minimal();
    configs.remove("spiffe.peer.allowed.ids");
    assertTrue(refusal(configs).contains("implicit"));
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    parse(configs);
  }

  @Test
  void refusesInvalidValues() {
    Object[][] cases = {
      {"spiffe.peer.allowed.ids", "spiffe://Example.org/a"},
      {"spiffe.peer.allowed.trust.domains", "spiffe://example.org"},
      {"spiffe.peer.allowed.id.patterns", "spiffe://example.org/web-*"},
      {"spiffe.svid.id", "not-an-id"},
      {"spiffe.bundle.trust.domains", "Example.org"},
      {"spiffe.svid.expired", "ignore"},
      {"spiffe.startup.timeout.ms", "-2"},
      {"spiffe.reconnect.backoff.ms", "0"},
      {"spiffe.reconnect.backoff.max.ms", "10"},
    };
    for (Object[] pair : cases) {
      Map<String, Object> configs = minimal();
      configs.put((String) pair[0], pair[1]);
      assertThrows(ConfigException.class, () -> parse(configs), pair[0] + "=" + pair[1]);
    }
  }
}
