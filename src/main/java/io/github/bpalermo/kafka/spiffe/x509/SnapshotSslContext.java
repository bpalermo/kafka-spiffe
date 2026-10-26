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
 * <p>A resumed session's peer is verified again. The JDK resumes a session without checking either
 * certificate, and no session timeout can stop a session made late in an SVID's life from being
 * resumed after the peer's has expired (a timeout counts from the session's creation, and a TLS 1.3
 * server hands out stateless tickets that nothing can invalidate). So the context's engines are the
 * JDK's wrapped: a handshake that finishes without the trust manager having verified the engine's
 * peer, as a resumption does, has the peer's certificates in the session verified by that trust
 * manager, at its clock, and fails if they are refused. Both session contexts also time sessions
 * out after {@value #SESSION_TIMEOUT_SECONDS} second, the shortest the JDK allows (it reads zero as
 * no limit), so resumptions stay rare. The context's sockets are the JDK's own and do not verify a
 * resumption again: use engines.
 *
 * <p>One context per snapshot: no handshake mixes the key of one snapshot with the trust of
 * another.
 *
 * <p>Public API (design, section 3).
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
    return create(snapshot, peers, settings, clock, SpiffeTrustManager.Observer.NONE);
  }

  /**
   * The context of {@code snapshot}, whose handshakes tell {@code observer} what the verification
   * of their peer came to.
   *
   * @param peers the SPIFFE IDs accepted of peers
   * @param clock the time peers' chains are validated at
   * @throws GeneralSecurityException if the JDK cannot make the context
   */
  public static SSLContext create(
      X509Context snapshot,
      SpiffeIdMatcher peers,
      Settings settings,
      Clock clock,
      SpiffeTrustManager.Observer observer)
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
    VerifyingSslContext.Engines engines = new VerifyingSslContext.Engines();
    SpiffeTrustManager trust =
        new SpiffeTrustManager(snapshot.bundles(), peers, clock, observer, engines);
    context.init(keyManagers.getKeyManagers(), new TrustManager[] {trust}, null);
    context.getClientSessionContext().setSessionTimeout(SESSION_TIMEOUT_SECONDS);
    context.getServerSessionContext().setSessionTimeout(SESSION_TIMEOUT_SECONDS);
    return new VerifyingSslContext(context, trust, engines);
  }
}
