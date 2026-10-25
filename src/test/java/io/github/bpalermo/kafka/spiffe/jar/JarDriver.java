package io.github.bpalermo.kafka.spiffe.jar;

import io.github.bpalermo.kafka.spiffe.testing.Handshake;
import java.net.InetAddress;
import java.nio.file.Path;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslEngineFactory;

/**
 * Run by {@link JarClassPathTest} in a JVM of its own, whose class path holds the published jar,
 * Kafka's jars and this driver, nothing of the build: it loads the factory and the principal
 * builder by class name, as Kafka does, makes a client and a broker engine on two Workload APIs,
 * handshakes, and builds the client's principal. It exits 0 when every check holds; a failed check
 * or a missing class is an exception, which exits 1.
 *
 * <p>Arguments: the published jar, the client's Workload API socket, the broker's.
 */
public final class JarDriver {

  private static final String FACTORY =
      "io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory";
  private static final String BUILDER =
      "io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder";
  private static final String BROKER = "spiffe://example.org/broker";
  private static final String CLIENT = "spiffe://k8s.example.org/ns/a/sa/b";

  private JarDriver() {}

  public static void main(String[] args) throws Exception {
    Path jar = Path.of(args[0]).toRealPath();
    Class<?> factoryClass = Class.forName(FACTORY);
    Path loadedFrom =
        Path.of(factoryClass.getProtectionDomain().getCodeSource().getLocation().toURI());
    check(jar.equals(loadedFrom.toRealPath()), "the factory was loaded from " + loadedFrom);

    Map<String, Object> client = new HashMap<>();
    client.put("spiffe.endpoint.socket", "unix://" + args[1]);
    client.put("spiffe.peer.allowed.ids", BROKER);
    client.put("spiffe.startup.timeout.ms", "10000");
    Map<String, Object> broker = new HashMap<>();
    broker.put("spiffe.endpoint.socket", "unix://" + args[2]);
    broker.put("spiffe.peer.allowed.trust.domains", "k8s.example.org");
    broker.put("spiffe.peer.allowed.ids", BROKER);
    broker.put("spiffe.startup.timeout.ms", "10000");
    broker.put("ssl.client.auth", "required");

    try (SslEngineFactory clientFactory = factory(factoryClass, client);
        SslEngineFactory brokerFactory = factory(factoryClass, broker)) {
      Handshake.Sessions sessions =
          Handshake.run(
              clientFactory.createClientSslEngine("broker.example.org", 9093, "https"),
              brokerFactory.createServerSslEngine("client.example.org", 50000));
      check(BROKER.equals(peer(sessions.client())), "the client saw " + peer(sessions.client()));
      check(CLIENT.equals(peer(sessions.server())), "the broker saw " + peer(sessions.server()));

      KafkaPrincipalBuilder builder =
          (KafkaPrincipalBuilder) Class.forName(BUILDER).getConstructor().newInstance();
      ((org.apache.kafka.common.Configurable) builder).configure(broker);
      KafkaPrincipal principal =
          builder.build(
              new SslAuthenticationContext(
                  sessions.server(), InetAddress.getLoopbackAddress(), "MTLS"));
      check(
          new KafkaPrincipal("Spiffe", CLIENT).equals(principal),
          "the client's principal is " + principal);
    }
  }

  private static SslEngineFactory factory(Class<?> factoryClass, Map<String, Object> configs)
      throws ReflectiveOperationException {
    SslEngineFactory factory = (SslEngineFactory) factoryClass.getConstructor().newInstance();
    factory.configure(configs);
    return factory;
  }

  /** The URI name of the peer's leaf certificate. */
  private static String peer(SSLSession session)
      throws SSLPeerUnverifiedException, CertificateParsingException {
    X509Certificate leaf = (X509Certificate) session.getPeerCertificates()[0];
    for (List<?> name : leaf.getSubjectAlternativeNames()) {
      if (Integer.valueOf(6).equals(name.get(0))) {
        return (String) name.get(1);
      }
    }
    return null;
  }

  private static void check(boolean condition, String failure) {
    if (!condition) {
      throw new IllegalStateException(failure);
    }
  }
}
