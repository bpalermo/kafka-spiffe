package io.github.bpalermo.kafka.spiffe.kafka;

import static io.github.bpalermo.kafka.spiffe.kafka.TlsPeers.client;
import static io.github.bpalermo.kafka.spiffe.kafka.TlsPeers.on;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The builder's principals before KRaft's StandardAuthorizer (3.3 and later; design, section 5):
 * ACLs on {@code Spiffe:} principals, {@code super.users}, mapping rules, the unauthorized
 * principal, and a principal forwarded to the controller through the serde.
 */
class StandardAuthorizerTest {

  private static final String WORKLOAD = "spiffe://k8s.example.org/ns/a/sa/b";
  private static final ResourcePattern ORDERS =
      new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);
  private static final ResourcePattern PAYMENTS =
      new ResourcePattern(ResourceType.TOPIC, "payments", PatternType.LITERAL);

  private StandardAuthorizer authorizer;
  private final Metrics metrics = new Metrics();

  @BeforeEach
  void authorizer() throws ReflectiveOperationException {
    authorizer = new StandardAuthorizer();
    withPluginMetrics(authorizer, metrics);
    authorizer.configure(Map.of("super.users", "Spiffe:spiffe://example.org/broker"));
    authorizer.completeInitialLoad();
    allow("Spiffe:" + WORKLOAD, ORDERS, AclOperation.READ);
    allow("Spiffe:spiffe-group:k8s.example.org:payments", PAYMENTS, AclOperation.WRITE);
  }

  @AfterEach
  void close() throws Exception {
    authorizer.close();
    metrics.close();
  }

  /**
   * What a 4.x broker does before using the authorizer (KIP-877), which 4.4's needs to authorize.
   * By reflection: the tests compile against 3.0, which has no plugin metrics.
   */
  private static void withPluginMetrics(StandardAuthorizer authorizer, Metrics metrics)
      throws ReflectiveOperationException {
    Class<?> pluginMetrics;
    try {
      pluginMetrics = Class.forName("org.apache.kafka.common.metrics.PluginMetrics");
    } catch (ClassNotFoundException e) {
      return;
    }
    Method with;
    try {
      with = StandardAuthorizer.class.getMethod("withPluginMetrics", pluginMetrics);
    } catch (NoSuchMethodException e) {
      return;
    }
    Object plugin =
        Class.forName("org.apache.kafka.common.metrics.internals.PluginMetricsImpl")
            .getConstructor(Metrics.class, Map.class)
            .newInstance(metrics, Map.of());
    with.invoke(authorizer, plugin);
  }

  private void allow(String principal, ResourcePattern resource, AclOperation operation) {
    authorizer.addAcl(
        Uuid.randomUuid(),
        StandardAcl.fromAclBinding(
            new AclBinding(
                resource,
                new AccessControlEntry(principal, "*", operation, AclPermissionType.ALLOW))));
  }

  private static Map<String, Object> configs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("spiffe.endpoint.socket", "unix:///run/spire/agent.sock");
    configs.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    configs.put("spiffe.principal.allowed.id.patterns", "spiffe://k8s.example.org/ns/*/sa/*");
    return configs;
  }

  private static KafkaPrincipal principal(Map<String, Object> configs, String path)
      throws Exception {
    SpiffePrincipalBuilder builder = new SpiffePrincipalBuilder(Map.of());
    builder.configure(configs);
    return builder.build(on("MTLS", client(path)));
  }

  private AuthorizationResult authorize(
      KafkaPrincipal principal, ResourcePattern resource, AclOperation operation) {
    return authorizer
        .authorize(
            new Request(principal), List.of(new Action(operation, resource, 1, false, false)))
        .get(0);
  }

  @Test
  void anAclOnTheSpiffePrincipalAllowsItsWorkloadOnly() throws Exception {
    KafkaPrincipal workload = principal(configs(), "/ns/a/sa/b");
    assertEquals(AuthorizationResult.ALLOWED, authorize(workload, ORDERS, AclOperation.READ));
    assertEquals(AuthorizationResult.DENIED, authorize(workload, ORDERS, AclOperation.WRITE));
    assertEquals(
        AuthorizationResult.DENIED,
        authorize(principal(configs(), "/ns/a/sa/c"), ORDERS, AclOperation.READ));
    // A SASL user of the same name does not match the workload's ACL.
    assertEquals(
        AuthorizationResult.DENIED,
        authorize(new KafkaPrincipal("User", WORKLOAD), ORDERS, AclOperation.READ));
  }

  @Test
  void theUnauthorizedPrincipalIsDeniedUnlessUserStarAllowsIt() throws Exception {
    KafkaPrincipal outsider = principal(configs(), "/batch/job");
    assertEquals(new KafkaPrincipal("Spiffe", "spiffe-unauthorized"), outsider);
    assertEquals(AuthorizationResult.DENIED, authorize(outsider, ORDERS, AclOperation.READ));
    // The caveat of design section 8: User:* matches principals of every type.
    allow("User:*", ORDERS, AclOperation.READ);
    assertEquals(AuthorizationResult.ALLOWED, authorize(outsider, ORDERS, AclOperation.READ));
  }

  @Test
  void aSuperUserNamedBySpiffeIdIsAllowedEverything() {
    KafkaPrincipal broker = new KafkaPrincipal("Spiffe", "spiffe://example.org/broker");
    assertEquals(AuthorizationResult.ALLOWED, authorize(broker, PAYMENTS, AclOperation.DELETE));
  }

  @Test
  void aMappingRuleGroupsANamespaceUnderOneAcl() throws Exception {
    Map<String, Object> configs = configs();
    configs.put(
        "spiffe.principal.mapping.rules",
        "RULE:^spiffe:\\/\\/k8s\\.example\\.org\\/ns\\/payments\\/sa\\/.*$"
            + "/spiffe-group:k8s.example.org:payments/, DEFAULT");
    for (String sa : List.of("api", "worker")) {
      assertEquals(
          AuthorizationResult.ALLOWED,
          authorize(principal(configs, "/ns/payments/sa/" + sa), PAYMENTS, AclOperation.WRITE));
    }
    assertEquals(
        AuthorizationResult.DENIED,
        authorize(principal(configs, "/ns/billing/sa/api"), PAYMENTS, AclOperation.WRITE));
  }

  @Test
  void aForwardedPrincipalIsAuthorizedAsTheOriginal() throws Exception {
    SpiffePrincipalBuilder builder = new SpiffePrincipalBuilder(Map.of());
    builder.configure(configs());
    KafkaPrincipal original = builder.build(on("MTLS", client("/ns/a/sa/b")));
    // The controller deserializes with the builder of its own configuration.
    KafkaPrincipal forwarded =
        new DefaultKafkaPrincipalBuilder(null, null).deserialize(builder.serialize(original));
    assertEquals(AuthorizationResult.ALLOWED, authorize(forwarded, ORDERS, AclOperation.READ));
  }

  /** A request of a principal on the MTLS listener. */
  private record Request(KafkaPrincipal principal) implements AuthorizableRequestContext {
    @Override
    public String listenerName() {
      return "MTLS";
    }

    @Override
    public SecurityProtocol securityProtocol() {
      return SecurityProtocol.SSL;
    }

    @Override
    public InetAddress clientAddress() {
      return InetAddress.getLoopbackAddress();
    }

    @Override
    public int requestType() {
      return 1;
    }

    @Override
    public int requestVersion() {
      return 0;
    }

    @Override
    public String clientId() {
      return "test";
    }

    @Override
    public int correlationId() {
      return 1;
    }
  }
}
