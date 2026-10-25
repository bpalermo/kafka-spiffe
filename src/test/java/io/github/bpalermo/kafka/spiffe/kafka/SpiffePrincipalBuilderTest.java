package io.github.bpalermo.kafka.spiffe.kafka;

import static io.github.bpalermo.kafka.spiffe.kafka.TlsPeers.client;
import static io.github.bpalermo.kafka.spiffe.kafka.TlsPeers.on;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.junit.jupiter.api.Test;

/** The principal builder on real TLS sessions, with no broker (design, section 5). */
class SpiffePrincipalBuilderTest {

  private static Map<String, Object> configs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("spiffe.endpoint.socket", "unix:///run/spire/agent.sock");
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    configs.put("spiffe.peer.allowed.ids", "spiffe://example.org/broker");
    return configs;
  }

  private static SpiffePrincipalBuilder builder(Map<String, Object> configs) {
    SpiffePrincipalBuilder builder = new SpiffePrincipalBuilder(Map.of());
    builder.configure(configs);
    return builder;
  }

  private static KafkaPrincipal principal(Map<String, Object> configs, String listener, String path)
      throws Exception {
    return builder(configs).build(on(listener, client(path)));
  }

  @Test
  void aTlsPeerIsItsSpiffeIdUnderTheSpiffeType() throws Exception {
    assertEquals(
        new KafkaPrincipal("Spiffe", "spiffe://k8s.example.org/ns/a/sa/b"),
        principal(configs(), "MTLS", "/ns/a/sa/b"));
    Map<String, Object> user = configs();
    user.put("spiffe.principal.type", "User");
    assertEquals(
        new KafkaPrincipal("User", "spiffe://k8s.example.org/ns/a/sa/b"),
        principal(user, "MTLS", "/ns/a/sa/b"));
  }

  @Test
  void thePrincipalIsComputedOncePerSession() throws Exception {
    SpiffePrincipalBuilder builder = builder(configs());
    SSLSession session = client("/ns/a/sa/b");
    KafkaPrincipal first = builder.build(on("MTLS", session));
    assertSame(first, builder.build(on("MTLS", session)));
    // Another session, even of the same peer, is read again.
    KafkaPrincipal other = builder.build(on("MTLS", client("/ns/a/sa/b")));
    assertNotSame(first, other);
    assertEquals(first, other);
    assertEquals(KafkaPrincipal.class, first.getClass());
  }

  @Test
  void aPeerOutsideThePrincipalAllowListIsUnauthorizedNotRefused() throws Exception {
    Map<String, Object> configs = configs();
    configs.put("spiffe.principal.allowed.id.patterns", "spiffe://k8s.example.org/ns/a/sa/*");
    assertEquals(
        new KafkaPrincipal("Spiffe", "spiffe://k8s.example.org/ns/a/sa/b"),
        principal(configs, "MTLS", "/ns/a/sa/b"));
    assertEquals(
        new KafkaPrincipal("Spiffe", "spiffe-unauthorized"),
        principal(configs, "MTLS", "/ns/other/sa/b"));
    configs.put("spiffe.principal.type", "User");
    assertEquals(
        new KafkaPrincipal("User", "spiffe-unauthorized"),
        principal(configs, "MTLS", "/ns/other/sa/b"));
  }

  @Test
  void withoutPrincipalKeysThePeerAllowListDecides() throws Exception {
    Map<String, Object> configs = configs();
    configs.put("spiffe.peer.allowed.trust.domains", "");
    configs.put("spiffe.peer.allowed.id.patterns", "spiffe://k8s.example.org/ns/a/**");
    assertEquals(
        "spiffe://k8s.example.org/ns/a/sa/b", principal(configs, "MTLS", "/ns/a/sa/b").getName());
    assertEquals("spiffe-unauthorized", principal(configs, "MTLS", "/ns/b/sa/b").getName());
  }

  @Test
  void aListenersOwnAllowListReplacesTheOthersThere() throws Exception {
    Map<String, Object> configs = configs();
    configs.put("spiffe.principal.allowed.trust.domains", "k8s.example.org");
    configs.put("spiffe.principal.listener.internal.allowed.ids", "spiffe://k8s.example.org/ops");
    assertEquals("spiffe://k8s.example.org/ops", principal(configs, "INTERNAL", "/ops").getName());
    assertEquals("spiffe-unauthorized", principal(configs, "INTERNAL", "/ns/a/sa/b").getName());
    assertEquals(
        "spiffe://k8s.example.org/ns/a/sa/b", principal(configs, "MTLS", "/ns/a/sa/b").getName());
  }

  @Test
  void mappingRulesUseKafkasSyntaxOnTheFullId() throws Exception {
    Map<String, Object> configs = configs();
    configs.put(
        "spiffe.principal.mapping.rules",
        "RULE:^spiffe:\\/\\/k8s\\.example\\.org\\/ns\\/([^\\/]+)\\/sa\\/.*$"
            + "/spiffe-group:k8s.example.org:$1/, "
            + "RULE:^spiffe:\\/\\/k8s\\.example\\.org\\/(OPS)$/$1/L, DEFAULT");
    assertEquals(
        new KafkaPrincipal("Spiffe", "spiffe-group:k8s.example.org:payments"),
        principal(configs, "MTLS", "/ns/payments/sa/api"));
    assertEquals("ops", principal(configs, "MTLS", "/OPS").getName());
    assertEquals(
        "spiffe://k8s.example.org/batch/job", principal(configs, "MTLS", "/batch/job").getName());

    configs.put(
        "spiffe.principal.mapping.rules",
        "RULE:^spiffe:\\/\\/k8s\\.example\\.org\\/ns\\/([^\\/]+)\\/.*$/$1/");
    assertEquals("spiffe-unauthorized", principal(configs, "MTLS", "/batch/job").getName());
  }

  @Test
  void aClientWithoutACertificateIsUnauthorized() throws Exception {
    assertEquals(
        new KafkaPrincipal("Spiffe", "spiffe-unauthorized"),
        builder(configs()).build(on("MTLS", TlsPeers.anonymous())));
  }

  @Test
  void plaintextIsAnonymousAndSaslIsKafkasOwn() {
    SpiffePrincipalBuilder builder = builder(configs());
    assertEquals(
        KafkaPrincipal.ANONYMOUS,
        builder.build(
            new PlaintextAuthenticationContext(InetAddress.getLoopbackAddress(), "CONTROLLER")));
    SaslServer plain = new PlainServer("spiffe://k8s.example.org/ns/a/sa/b");
    SaslAuthenticationContext sasl =
        new SaslAuthenticationContext(
            plain, SecurityProtocol.SASL_SSL, InetAddress.getLoopbackAddress(), "SASL");
    // A SASL user of the same name is a User, never a Spiffe principal.
    assertEquals(
        new KafkaPrincipal("User", "spiffe://k8s.example.org/ns/a/sa/b"), builder.build(sasl));
  }

  @Test
  void theSerdeIsKafkasSoAForwardedPrincipalIsTheSame() throws Exception {
    SpiffePrincipalBuilder builder = builder(configs());
    KafkaPrincipal principal = builder.build(on("MTLS", client("/ns/a/sa/b")));
    DefaultKafkaPrincipalBuilder kafka = new DefaultKafkaPrincipalBuilder(null, null);
    byte[] bytes = builder.serialize(principal);
    assertEquals(principal, kafka.deserialize(bytes));
    KafkaPrincipal back = builder.deserialize(kafka.serialize(principal));
    assertEquals(principal, back);
    assertEquals(KafkaPrincipal.class, back.getClass());
  }

  @Test
  void buildersOfOneConfigurationShareItsPolicy() {
    assertSame(builder(configs()).policy(), builder(configs()).policy());
    Map<String, Object> other = configs();
    other.put("spiffe.principal.type", "User");
    assertNotSame(builder(configs()).policy(), builder(other).policy());
  }

  @Test
  void badPrincipalSettingsAreConfigurationErrors() {
    Map<String, Object> type = configs();
    type.put("spiffe.principal.type", "Spiffe:x");
    assertThrows(ConfigException.class, () -> builder(type));

    Map<String, Object> rules = configs();
    rules.put("spiffe.principal.mapping.rules", "RULE:unterminated");
    ConfigException e = assertThrows(ConfigException.class, () -> builder(rules));
    assertTrue(e.getMessage().contains("spiffe.principal.mapping.rules"), e.getMessage());

    Map<String, Object> pattern = configs();
    pattern.put("spiffe.principal.listener.mtls.allowed.id.patterns", "spiffe://example.org/w-*");
    e = assertThrows(ConfigException.class, () -> builder(pattern));
    assertTrue(e.getMessage().contains("spiffe.principal.listener.mtls"), e.getMessage());

    Map<String, Object> spelled = configs();
    spelled.put("spiffe.principal.listener.mtls.allowed.ids", "spiffe://example.org/a");
    spelled.put("spiffe.principal.listener.MTLS.allowed.trust.domains", "example.org");
    assertThrows(ConfigException.class, () -> builder(spelled));
  }

  @Test
  void theLogLimiterCountsWhatItDrops() {
    AtomicLong now = new AtomicLong();
    LogLimiter limiter = new LogLimiter(2, 1_000, now::get);
    assertEquals(0, limiter.acquire());
    assertEquals(0, limiter.acquire());
    assertEquals(-1, limiter.acquire());
    assertEquals(-1, limiter.acquire());
    now.set(1_000_000_000L);
    assertEquals(2, limiter.acquire());
    assertEquals(0, limiter.acquire());
  }

  /** A completed SASL/PLAIN exchange. */
  private record PlainServer(String user) implements SaslServer {
    @Override
    public String getMechanismName() {
      return "PLAIN";
    }

    @Override
    public byte[] evaluateResponse(byte[] response) {
      return new byte[0];
    }

    @Override
    public boolean isComplete() {
      return true;
    }

    @Override
    public String getAuthorizationID() {
      return user;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
      throw new IllegalStateException();
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
      throw new IllegalStateException();
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
      return null;
    }

    @Override
    public void dispose() {}
  }
}
