package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * The JDK context of one snapshot: it presents the snapshot's SVID and verifies peers with a {@link
 * SpiffeTrustManager} over the snapshot's bundles.
 *
 * <p>One context per snapshot means one session cache per snapshot: a session is never resumed
 * across a rotation, and no handshake mixes the key of one snapshot with the trust of another. The
 * session timeout is capped at the SVID's remaining lifetime, so no session outlives it.
 */
public final class SnapshotSslContext {

  /** In-memory key store entries need a password; it protects nothing. */
  private static final char[] PASSWORD = "in-memory".toCharArray();

  private static final String ALIAS = "svid";

  /**
   * How the context is made.
   *
   * @param protocol the {@link SSLContext} protocol, such as {@code TLSv1.3} or {@code TLS}
   * @param provider the JCA provider of the context, or null for the default
   * @param sessionTimeout the longest a session may be resumed, before the SVID's lifetime caps it
   */
  public record Settings(String protocol, String provider, Duration sessionTimeout) {

    /** TLS from the default provider, sessions of at most an hour. */
    public static final Settings DEFAULT = new Settings("TLS", null, Duration.ofHours(1));

    public Settings {
      if (protocol == null || protocol.isEmpty()) {
        throw new IllegalArgumentException("a protocol is required");
      }
      if (sessionTimeout == null || sessionTimeout.isNegative() || sessionTimeout.isZero()) {
        throw new IllegalArgumentException("the session timeout must be positive");
      }
    }
  }

  private SnapshotSslContext() {}

  /**
   * The context of {@code snapshot}.
   *
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time chains are validated at and the SVID's remaining lifetime measured from
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
    int timeout =
        sessionTimeoutSeconds(settings.sessionTimeout(), svid.expiresAt(), clock.instant());
    context.getClientSessionContext().setSessionTimeout(timeout);
    context.getServerSessionContext().setSessionTimeout(timeout);
    return context;
  }

  /**
   * The session timeout in seconds: the configured one, capped at what is left of the SVID's
   * lifetime, and never below one second, since the JDK reads zero as no limit at all.
   */
  static int sessionTimeoutSeconds(Duration configured, Instant expiresAt, Instant now) {
    Duration left = Duration.between(now, expiresAt);
    Duration timeout = left.compareTo(configured) < 0 ? left : configured;
    long seconds = Math.max(1, timeout.getSeconds());
    return (int) Math.min(Integer.MAX_VALUE, seconds);
  }
}
