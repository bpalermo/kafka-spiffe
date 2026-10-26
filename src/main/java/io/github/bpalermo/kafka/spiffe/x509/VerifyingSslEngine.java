package io.github.bpalermo.kafka.spiffe.x509;

import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * An engine of a {@link SnapshotSslContext} context: the JDK's, which verifies the peer again when
 * a handshake ends without the trust manager having run, as a resumed session's does.
 *
 * <p>A resumption skips the JDK's certificate checks, so a session made while both SVIDs were valid
 * could otherwise be resumed after the peer's had expired. When the handshake finishes (or, should
 * the JDK not say so, before the first byte of application data) and the context's trust manager
 * has not verified the peer of this engine, the peer's certificates in the session are verified by
 * that trust manager, at its clock, and a refusal fails the handshake: the bytes of that last step
 * are withheld, the outbound side is closed, and the exception is thrown again on reading.
 *
 * <p>Withheld means overwritten with zeros and taken back: each destination buffer's position goes
 * back to where it was. A refused {@code wrap} also sets its buffer's limit there, so that a caller
 * that sends the buffer from its position to its limit without flipping it, as Kafka 3.0's {@code
 * SslTransportLayer} does after a failed handshake step, sends nothing.
 *
 * <p>Each handshake is checked once it finishes: the first, and a TLS 1.2 renegotiation this engine
 * begins with {@link #beginHandshake}. On TLS 1.3, {@code beginHandshake} on an established
 * connection sends a KeyUpdate, which is no handshake and verifies nobody, so it is not checked: a
 * long-lived connection is not refused because its peer's SVID has expired since it was made. A TLS
 * 1.2 renegotiation the peer starts is not checked again; Kafka refuses renegotiation, and it is a
 * full handshake unless it resumes a session.
 */
final class VerifyingSslEngine extends SSLEngine {

  private final SSLEngine engine;
  private final SpiffeTrustManager trust;
  private final AtomicBoolean verifiedInHandshake;
  private boolean checked;
  private boolean verifiedAgain;
  private SSLHandshakeException refused;

  /**
   * @param verifiedInHandshake set by the trust manager when it verifies this engine's peer
   */
  VerifyingSslEngine(
      SSLEngine engine, SpiffeTrustManager trust, AtomicBoolean verifiedInHandshake) {
    super(engine.getPeerHost(), engine.getPeerPort());
    this.engine = engine;
    this.trust = trust;
    this.verifiedInHandshake = verifiedInHandshake;
  }

  /** Whether the peer was verified here rather than by the handshake: a resumed session's. */
  synchronized boolean verifiedAgain() {
    return verifiedAgain;
  }

  @Override
  public synchronized SSLEngineResult wrap(
      ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException {
    int position = dst.position();
    SSLEngineResult result = engine.wrap(srcs, offset, length, dst);
    if (!checked
        && (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
            || result.bytesConsumed() > 0)) {
      try {
        check();
      } catch (SSLException e) {
        withhold(dst, position);
        dst.limit(position);
        throw e;
      }
    }
    return result;
  }

  @Override
  public synchronized SSLEngineResult unwrap(
      ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException {
    if (refused != null) {
      throw refused;
    }
    int[] positions = new int[length];
    for (int i = 0; i < length; i++) {
      positions[i] = dsts[offset + i].position();
    }
    SSLEngineResult result = engine.unwrap(src, dsts, offset, length);
    if (!checked
        && (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
            || result.bytesProduced() > 0)) {
      try {
        check();
      } catch (SSLException e) {
        for (int i = 0; i < length; i++) {
          withhold(dsts[offset + i], positions[i]);
        }
        throw e;
      }
    }
    return result;
  }

  /** Overwrites what was written to {@code buffer} since {@code position}, and takes it back. */
  private static void withhold(ByteBuffer buffer, int position) {
    for (int i = position; i < buffer.position(); i++) {
      buffer.put(i, (byte) 0);
    }
    buffer.position(position);
  }

  /** Verifies the peer again unless the handshake's trust manager did. */
  private void check() throws SSLHandshakeException {
    checked = true;
    if (verifiedInHandshake.get()) {
      return;
    }
    boolean peerIsServer = engine.getUseClientMode();
    SSLSession session = engine.getSession();
    Certificate[] chain;
    try {
      chain = session.getPeerCertificates();
    } catch (SSLPeerUnverifiedException e) {
      if (peerIsServer || engine.getNeedClientAuth()) {
        refuse(
            new PeerRejectedException(
                PeerRejectedException.Reason.MALFORMED_SVID, "the peer presented no certificate"));
      }
      // A client that was not required to authenticate did not.
      return;
    }
    X509Certificate[] x509 = new X509Certificate[chain.length];
    for (int i = 0; i < chain.length; i++) {
      if (!(chain[i] instanceof X509Certificate certificate)) {
        refuse(
            new PeerRejectedException(
                PeerRejectedException.Reason.MALFORMED_SVID,
                "the peer's certificate is not an X.509 certificate"));
        return;
      }
      x509[i] = certificate;
    }
    verifiedAgain = true;
    try {
      trust.verifyAgain(x509, peerIsServer, engine.getSSLParameters().getAlgorithmConstraints());
    } catch (PeerRejectedException e) {
      refuse(e);
    }
  }

  private void refuse(PeerRejectedException rejection) throws SSLHandshakeException {
    SSLHandshakeException e =
        new SSLHandshakeException(
            "the peer of a resumed session is refused: " + rejection.getMessage());
    e.initCause(rejection);
    refused = e;
    engine.closeOutbound();
    throw e;
  }

  @Override
  public Runnable getDelegatedTask() {
    return engine.getDelegatedTask();
  }

  @Override
  public void closeInbound() throws SSLException {
    engine.closeInbound();
  }

  @Override
  public boolean isInboundDone() {
    return engine.isInboundDone();
  }

  @Override
  public void closeOutbound() {
    engine.closeOutbound();
  }

  @Override
  public boolean isOutboundDone() {
    return engine.isOutboundDone();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return engine.getSupportedCipherSuites();
  }

  @Override
  public String[] getEnabledCipherSuites() {
    return engine.getEnabledCipherSuites();
  }

  @Override
  public void setEnabledCipherSuites(String[] suites) {
    engine.setEnabledCipherSuites(suites);
  }

  @Override
  public String[] getSupportedProtocols() {
    return engine.getSupportedProtocols();
  }

  @Override
  public String[] getEnabledProtocols() {
    return engine.getEnabledProtocols();
  }

  @Override
  public void setEnabledProtocols(String[] protocols) {
    engine.setEnabledProtocols(protocols);
  }

  @Override
  public SSLSession getSession() {
    return engine.getSession();
  }

  @Override
  public SSLSession getHandshakeSession() {
    return engine.getHandshakeSession();
  }

  @Override
  public synchronized void beginHandshake() throws SSLException {
    if (checked && !"TLSv1.3".equals(engine.getSession().getProtocol())) {
      // A renegotiation: checked when it finishes, as the first handshake was. TLS 1.3 has none;
      // there this is a KeyUpdate.
      checked = false;
      verifiedInHandshake.set(false);
    }
    engine.beginHandshake();
  }

  @Override
  public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
    return engine.getHandshakeStatus();
  }

  @Override
  public void setUseClientMode(boolean mode) {
    engine.setUseClientMode(mode);
  }

  @Override
  public boolean getUseClientMode() {
    return engine.getUseClientMode();
  }

  @Override
  public void setNeedClientAuth(boolean need) {
    engine.setNeedClientAuth(need);
  }

  @Override
  public boolean getNeedClientAuth() {
    return engine.getNeedClientAuth();
  }

  @Override
  public void setWantClientAuth(boolean want) {
    engine.setWantClientAuth(want);
  }

  @Override
  public boolean getWantClientAuth() {
    return engine.getWantClientAuth();
  }

  @Override
  public void setEnableSessionCreation(boolean flag) {
    engine.setEnableSessionCreation(flag);
  }

  @Override
  public boolean getEnableSessionCreation() {
    return engine.getEnableSessionCreation();
  }

  @Override
  public SSLParameters getSSLParameters() {
    return engine.getSSLParameters();
  }

  @Override
  public void setSSLParameters(SSLParameters params) {
    engine.setSSLParameters(params);
  }

  @Override
  public String getApplicationProtocol() {
    return engine.getApplicationProtocol();
  }

  @Override
  public String getHandshakeApplicationProtocol() {
    return engine.getHandshakeApplicationProtocol();
  }

  @Override
  public void setHandshakeApplicationProtocolSelector(
      BiFunction<SSLEngine, List<String>, String> selector) {
    engine.setHandshakeApplicationProtocolSelector(selector);
  }

  @Override
  public BiFunction<SSLEngine, List<String>, String> getHandshakeApplicationProtocolSelector() {
    return engine.getHandshakeApplicationProtocolSelector();
  }

  @Override
  public String toString() {
    return engine.toString();
  }
}
