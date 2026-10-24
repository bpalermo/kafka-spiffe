package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Clock;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * The JDK context of one snapshot: it presents the snapshot's SVID and verifies peers with a {@link
 * SpiffeTrustManager} over the snapshot's bundles.
 *
 * <p>Sessions are not resumed: both session contexts time sessions out after {@value
 * #SESSION_TIMEOUT_SECONDS} second, the shortest the JDK allows (it reads zero as no limit). A
 * session timeout counts from each session's creation, so no timeout can stop a session made late
 * in an SVID's life from being resumed after that SVID, or the peer's, has expired, without either
 * certificate being checked again; and a TLS 1.3 server hands out stateless tickets that no cache
 * holds and nothing can invalidate. Kafka's connections are long-lived, so a full handshake on
 * reconnecting costs little.
 *
 * <p>One context per snapshot: no handshake mixes the key of one snapshot with the trust of
 * another.
 */
public final class SnapshotSslContext {

  /** The shortest session timeout the JDK has; zero would mean sessions never time out. */
  static final int SESSION_TIMEOUT_SECONDS = 1;

  /** In-memory key store entries need a password; it protects nothing. */
  private static final char[] PASSWORD = "in-memory".toCharArray();

  private static final String ALIAS = "svid";

  /**
   * How the context is made.
   *
   * @param protocol the {@link SSLContext} protocol, such as {@code TLSv1.3} or {@code TLS}
   * @param provider the JCA provider of the context, or null for the default
   */
  public record Settings(String protocol, String provider) {

    /** TLS from the default provider. */
    public static final Settings DEFAULT = new Settings("TLS", null);

    public Settings {
      if (protocol == null || protocol.isEmpty()) {
        throw new IllegalArgumentException("a protocol is required");
      }
    }
  }

  private SnapshotSslContext() {}

  /**
   * The context of {@code snapshot}.
   *
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time peers' chains are validated at
   * @throws GeneralSecurityException if the JDK cannot make the context
   */
  public static SSLContext create(
      X509Context snapshot, SpiffeIdMatcher peers, Settings settings, Clock clock)
      throws GeneralSecurityException {
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try {
      keys.load(null, null);
    } catch (IOException e) {
      throw new GeneralSecurityException("cannot create an in-memory key store", e);
    }
    X509Svid svid = snapshot.svid();
    keys.setKeyEntry(ALIAS, svid.key(), PASSWORD, svid.chain().toArray(X509Certificate[]::new));
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, PASSWORD);

    SSLContext context =
        settings.provider() == null
            ? SSLContext.getInstance(settings.protocol())
            : SSLContext.getInstance(settings.protocol(), settings.provider());
    context.init(
        keyManagers.getKeyManagers(),
        new TrustManager[] {new SpiffeTrustManager(snapshot.bundles(), peers, clock)},
        null);
    context.getClientSessionContext().setSessionTimeout(SESSION_TIMEOUT_SECONDS);
    context.getServerSessionContext().setSessionTimeout(SESSION_TIMEOUT_SECONDS);
    return context;
  }
}
