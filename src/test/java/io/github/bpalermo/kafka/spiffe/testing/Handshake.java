package io.github.bpalermo.kafka.spiffe.testing;

import java.nio.ByteBuffer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

/**
 * A TLS handshake between a client and a server {@link SSLEngine}, in memory, with no sockets: the
 * bytes one engine wraps are what the other unwraps.
 */
public final class Handshake {

  private static final int MAX_ROUNDS = 1000;

  /** The two sides' sessions once the handshake is done. */
  public record Sessions(SSLSession client, SSLSession server) {}

  private Handshake() {}

  /**
   * Runs a handshake in which the server requires a client certificate.
   *
   * @throws SSLException if either side refuses the other
   */
  public static Sessions run(SSLContext client, SSLContext server) throws SSLException {
    SSLEngine clientEngine = client.createSSLEngine("", 0);
    clientEngine.setUseClientMode(true);
    SSLEngine serverEngine = server.createSSLEngine();
    serverEngine.setUseClientMode(false);
    serverEngine.setNeedClientAuth(true);
    return run(clientEngine, serverEngine);
  }

  /**
   * Runs a handshake between two engines already set up.
   *
   * @throws SSLException if either side refuses the other
   */
  public static Sessions run(SSLEngine client, SSLEngine server) throws SSLException {
    int packet =
        Math.max(
            client.getSession().getPacketBufferSize(), server.getSession().getPacketBufferSize());
    int application =
        Math.max(
            client.getSession().getApplicationBufferSize(),
            server.getSession().getApplicationBufferSize());
    ByteBuffer toServer = ByteBuffer.allocate(packet);
    ByteBuffer toClient = ByteBuffer.allocate(packet);
    ByteBuffer empty = ByteBuffer.allocate(0);
    ByteBuffer sink = ByteBuffer.allocate(application);
    client.beginHandshake();
    server.beginHandshake();
    for (int round = 0; round < MAX_ROUNDS; round++) {
      if (done(client) && done(server)) {
        return new Sessions(client.getSession(), server.getSession());
      }
      step(client, toServer, toClient, empty, sink);
      step(server, toClient, toServer, empty, sink);
    }
    throw new SSLException("the handshake did not finish in " + MAX_ROUNDS + " rounds");
  }

  private static boolean done(SSLEngine engine) {
    HandshakeStatus status = engine.getHandshakeStatus();
    return status == HandshakeStatus.NOT_HANDSHAKING || status == HandshakeStatus.FINISHED;
  }

  /** Lets one engine send what it has to {@code out}, and read what waits in {@code in}. */
  private static void step(
      SSLEngine engine, ByteBuffer out, ByteBuffer in, ByteBuffer empty, ByteBuffer sink)
      throws SSLException {
    HandshakeStatus status = engine.getHandshakeStatus();
    while (status == HandshakeStatus.NEED_TASK) {
      Runnable task;
      while ((task = engine.getDelegatedTask()) != null) {
        task.run();
      }
      status = engine.getHandshakeStatus();
    }
    if (status == HandshakeStatus.NEED_WRAP) {
      SSLEngineResult result = engine.wrap(empty, out);
      if (result.getStatus() == SSLEngineResult.Status.CLOSED && engine.isOutboundDone()) {
        throw new SSLException("the engine closed during the handshake");
      }
    } else if (status == HandshakeStatus.NEED_UNWRAP && in.position() > 0) {
      in.flip();
      sink.clear();
      engine.unwrap(in, sink);
      in.compact();
    }
  }
}
