package io.github.bpalermo.kafka.spiffe.x509;

import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * The context {@link SnapshotSslContext} returns: the JDK's, initialised, whose engines are {@link
 * VerifyingSslEngine}s. Its sockets are the JDK's own.
 */
final class VerifyingSslContext extends SSLContext {

  /**
   * The engines whose handshake is not yet checked, by the JDK engine the trust manager is handed,
   * each with the flag it sets. The values hold no engine, so an engine dropped mid-handshake is
   * collected.
   */
  static final class Engines {
    private final Map<SSLEngine, AtomicBoolean> pending =
        Collections.synchronizedMap(new WeakHashMap<>());

    /** The trust manager verified the peer of {@code engine}. */
    void verified(SSLEngine engine) {
      AtomicBoolean flag = engine == null ? null : pending.get(engine);
      if (flag != null) {
        flag.set(true);
      }
    }

    VerifyingSslEngine wrap(SSLEngine engine, SpiffeTrustManager trust) {
      AtomicBoolean flag = new AtomicBoolean();
      pending.put(engine, flag);
      return new VerifyingSslEngine(engine, trust, flag, () -> pending.remove(engine));
    }
  }

  VerifyingSslContext(SSLContext context, SpiffeTrustManager trust, Engines engines) {
    super(new Spi(context, trust, engines), context.getProvider(), context.getProtocol());
  }

  private static final class Spi extends SSLContextSpi {
    private final SSLContext context;
    private final SpiffeTrustManager trust;
    private final Engines engines;

    Spi(SSLContext context, SpiffeTrustManager trust, Engines engines) {
      this.context = context;
      this.trust = trust;
      this.engines = engines;
    }

    @Override
    protected void engineInit(KeyManager[] keys, TrustManager[] trusts, SecureRandom random)
        throws KeyManagementException {
      throw new KeyManagementException("a snapshot's context is initialised once, when made");
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
      return context.getSocketFactory();
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
      return context.getServerSocketFactory();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
      return engines.wrap(context.createSSLEngine(), trust);
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String host, int port) {
      return engines.wrap(context.createSSLEngine(host, port), trust);
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
      return context.getServerSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
      return context.getClientSessionContext();
    }

    @Override
    protected SSLParameters engineGetDefaultSSLParameters() {
      return context.getDefaultSSLParameters();
    }

    @Override
    protected SSLParameters engineGetSupportedSSLParameters() {
      return context.getSupportedSSLParameters();
    }
  }
}
