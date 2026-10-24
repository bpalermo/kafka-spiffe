package io.github.bpalermo.kafka.spiffe.workload.h2;

import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcStatus.Code;
import io.github.bpalermo.kafka.spiffe.workload.h2.Hpack.Field;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * One server-streaming gRPC call over HTTP/2 with prior knowledge, on a connection of its own: the
 * client side of exactly what the Workload API's {@code FetchX509SVID} needs (design, section 3).
 *
 * <p>{@link #run} sends the request and then reads on the calling thread until the call ends,
 * handing each response message to the consumer as it completes, and returns the call's status. It
 * answers pings, acknowledges settings and returns flow-control credit as it reads, so nothing else
 * writes to the connection. {@link #close} may be called from any thread; a running call then ends
 * as {@link Code#CANCELLED}.
 *
 * <p>Errors this client detects are sent with the code and scope RFC 9113 requires; errors the
 * server sends are translated as gRPC's HTTP/2 mapping says. An explicit {@code grpc-status} always
 * decides the status; the HTTP status decides only without one.
 */
public final class GrpcCall implements Closeable {

  /** The flow-control window this client gives the connection and its stream. */
  static final int WINDOW = 1 << 20;

  /** The largest response message accepted. */
  static final int MAX_MESSAGE = 16 << 20;

  /** The largest header list accepted, in RFC 7541 octets. */
  static final int MAX_HEADER_LIST = 64 << 10;

  /** The dynamic table size this client allows the server's encoder: the default, unchanged. */
  private static final int HEADER_TABLE_SIZE = 4096;

  private static final int STREAM = 1;

  private final ByteChannel channel;
  private final ByteBuffer in = ByteBuffer.allocate(64 << 10).flip();
  private volatile boolean closed;

  GrpcCall(ByteChannel channel) {
    this.channel = channel;
  }

  /**
   * Connects to a server on a Unix domain socket.
   *
   * @throws IOException if the socket cannot be connected to
   */
  public static GrpcCall connect(Path socket) throws IOException {
    SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
    try {
      channel.connect(UnixDomainSocketAddress.of(socket));
    } catch (IOException e) {
      channel.close();
      throw e;
    }
    return new GrpcCall(channel);
  }

  /**
   * Runs the call: sends {@code request} to {@code path} with {@code metadata}, hands each response
   * message to {@code onMessage}, and returns the status the call ended with. The connection is
   * closed when it returns. An exception from {@code onMessage} closes it too, and is rethrown.
   *
   * @param path the method, such as {@code /SpiffeWorkloadAPI/FetchX509SVID}
   * @param metadata extra request headers, lowercase names, in order
   * @param request the serialised request message
   */
  public GrpcStatus run(
      String path, Map<String, String> metadata, byte[] request, Consumer<byte[]> onMessage) {
    try {
      sendRequest(path, metadata, request);
      GrpcStatus status = new Reader(onMessage).read();
      return closed ? GrpcStatus.of(Code.CANCELLED, "the call was closed") : status;
    } catch (Http2.ConnectionError e) {
      goAway(e.code());
      return GrpcStatus.of(Code.INTERNAL, "HTTP/2 connection error: " + e.getMessage());
    } catch (IOException e) {
      if (closed) {
        return GrpcStatus.of(Code.CANCELLED, "the call was closed");
      }
      return GrpcStatus.of(Code.UNAVAILABLE, "the connection failed: " + e);
    } finally {
      close();
    }
  }

  /** Closes the connection; a running call ends as CANCELLED. */
  @Override
  public void close() {
    closed = true;
    try {
      channel.close();
    } catch (IOException e) {
      // Nothing is left to do with a connection that cannot even be closed.
    }
  }

  private void sendRequest(String path, Map<String, String> metadata, byte[] request)
      throws IOException {
    ByteBuffer settings = ByteBuffer.allocate(12);
    settings.putShort((short) Http2.SETTINGS_ENABLE_PUSH).putInt(0);
    settings.putShort((short) Http2.SETTINGS_INITIAL_WINDOW_SIZE).putInt(WINDOW);
    write(ByteBuffer.wrap(Http2.PREFACE));
    writeFrame(Http2.SETTINGS, 0, 0, settings.array());
    writeFrame(Http2.WINDOW_UPDATE, 0, 0, int32(WINDOW - Http2.DEFAULT_WINDOW));

    List<Field> fields = new ArrayList<>();
    fields.add(new Field(":method", "POST"));
    fields.add(new Field(":scheme", "http"));
    fields.add(new Field(":path", path));
    fields.add(new Field(":authority", "localhost"));
    fields.add(new Field("content-type", "application/grpc"));
    fields.add(new Field("te", "trailers"));
    fields.add(new Field("grpc-accept-encoding", "identity"));
    metadata.forEach((name, value) -> fields.add(new Field(name, value)));
    byte[] block = Hpack.encode(fields);
    int offset = 0;
    boolean first = true;
    do {
      int length = Math.min(Http2.MAX_FRAME_SIZE, block.length - offset);
      boolean last = offset + length == block.length;
      byte[] fragment = java.util.Arrays.copyOfRange(block, offset, offset + length);
      writeFrame(
          first ? Http2.HEADERS : Http2.CONTINUATION,
          last ? Http2.FLAG_END_HEADERS : 0,
          STREAM,
          fragment);
      offset += length;
      first = false;
    } while (offset < block.length);

    // A request this small fits the 65,535 octets every peer allows until its SETTINGS arrive.
    if (request.length + 5 > Math.min(Http2.MAX_FRAME_SIZE, Http2.DEFAULT_WINDOW)) {
      throw new IllegalArgumentException("a request of " + request.length + " octets is too large");
    }
    ByteBuffer message = ByteBuffer.allocate(5 + request.length);
    message.put((byte) 0).putInt(request.length).put(request);
    writeFrame(Http2.DATA, Http2.FLAG_END_STREAM, STREAM, message.array());
  }

  private void goAway(int code) {
    try {
      ByteBuffer payload = ByteBuffer.allocate(8);
      payload.putInt(STREAM).putInt(code);
      writeFrame(Http2.GOAWAY, 0, 0, payload.array());
    } catch (IOException e) {
      // The connection is being closed for an error already.
    }
  }

  private void writeFrame(int type, int flags, int stream, byte[] payload) throws IOException {
    ByteBuffer frame = ByteBuffer.allocate(Http2.FRAME_HEADER_LENGTH + payload.length);
    frame.put((byte) (payload.length >>> 16)).put((byte) (payload.length >>> 8));
    frame.put((byte) payload.length).put((byte) type).put((byte) flags).putInt(stream);
    frame.put(payload).flip();
    write(frame);
  }

  private void write(ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private static byte[] int32(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  /** Reads exactly {@code length} octets. */
  private byte[] readFully(int length) throws IOException {
    byte[] out = new byte[length];
    int done = 0;
    while (done < length) {
      if (!in.hasRemaining()) {
        in.clear();
        int read = channel.read(in);
        in.flip();
        if (read < 0) {
          throw new IOException("the server closed the connection");
        }
      }
      int n = Math.min(in.remaining(), length - done);
      in.get(out, done, n);
      done += n;
    }
    return out;
  }

  /** The state of reading one call's response. */
  private final class Reader {
    private final Consumer<byte[]> onMessage;
    private final Hpack.Decoder decoder = new Hpack.Decoder(HEADER_TABLE_SIZE, MAX_HEADER_LIST);
    private final byte[] prefix = new byte[5];
    private int prefixFilled;
    private byte[] body;
    private int bodyFilled;
    private ByteArrayOutputStream headerBlock;
    private boolean headerBlockEndsStream;
    private boolean settingsSeen;
    private boolean responseHeadersSeen;
    private int httpStatus;
    private boolean grpcResponse;
    private long connectionWindow = WINDOW;
    private long streamWindow = WINDOW;
    private int connectionConsumed;
    private int streamConsumed;

    Reader(Consumer<byte[]> onMessage) {
      this.onMessage = onMessage;
    }

    GrpcStatus read() throws IOException, Http2.ConnectionError {
      while (true) {
        byte[] header = readFully(Http2.FRAME_HEADER_LENGTH);
        int length = ((header[0] & 0xff) << 16) | ((header[1] & 0xff) << 8) | (header[2] & 0xff);
        int type = header[3] & 0xff;
        int flags = header[4] & 0xff;
        int stream = ByteBuffer.wrap(header, 5, 4).getInt() & 0x7fffffff;
        if (length > Http2.MAX_FRAME_SIZE) {
          throw new Http2.ConnectionError(
              Http2.FRAME_SIZE_ERROR, "a frame of " + length + " octets");
        }
        byte[] payload = readFully(length);
        if (!settingsSeen && type != Http2.SETTINGS) {
          throw new Http2.ConnectionError(
              Http2.PROTOCOL_ERROR, "the server's first frame is not SETTINGS");
        }
        if (headerBlock != null && (type != Http2.CONTINUATION || stream != STREAM)) {
          throw new Http2.ConnectionError(
              Http2.PROTOCOL_ERROR, "a frame of type " + type + " inside a header block");
        }
        if (closed) {
          return GrpcStatus.of(Code.CANCELLED, "the call was closed");
        }
        GrpcStatus end = frame(type, flags, stream, payload);
        if (end != null) {
          return end;
        }
      }
    }

    /** Handles one frame; returns the call's status once it has ended. */
    private GrpcStatus frame(int type, int flags, int stream, byte[] payload)
        throws IOException, Http2.ConnectionError {
      switch (type) {
        case Http2.SETTINGS -> settings(flags, stream, payload);
        case Http2.PING -> ping(flags, stream, payload);
        case Http2.WINDOW_UPDATE -> windowUpdate(payload);
        case Http2.GOAWAY -> {
          return goAway(stream, payload);
        }
        case Http2.RST_STREAM -> {
          requireLength(payload, 4, "RST_STREAM");
          requireOurStream(stream, "RST_STREAM");
          return GrpcStatus.ofReset(ByteBuffer.wrap(payload).getInt() & 0xffffffffL);
        }
        case Http2.PRIORITY -> {
          if (stream == 0) {
            throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "PRIORITY on stream 0");
          }
          requireLength(payload, 5, "PRIORITY");
        }
        case Http2.PUSH_PROMISE ->
            throw new Http2.ConnectionError(
                Http2.PROTOCOL_ERROR, "PUSH_PROMISE, which this client disabled");
        case Http2.HEADERS -> {
          return headers(flags, stream, payload);
        }
        case Http2.CONTINUATION -> {
          return continuation(flags, payload);
        }
        case Http2.DATA -> {
          return data(flags, stream, payload);
        }
        default -> {
          // Unknown frame types are ignored (RFC 9113 section 4.1).
        }
      }
      return null;
    }

    private void settings(int flags, int stream, byte[] payload)
        throws IOException, Http2.ConnectionError {
      if (stream != 0) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "SETTINGS on stream " + stream);
      }
      if ((flags & Http2.FLAG_ACK) != 0) {
        if (payload.length != 0) {
          throw new Http2.ConnectionError(Http2.FRAME_SIZE_ERROR, "a SETTINGS ack with a payload");
        }
        return;
      }
      if (payload.length % 6 != 0) {
        throw new Http2.ConnectionError(
            Http2.FRAME_SIZE_ERROR, "SETTINGS of " + payload.length + " octets");
      }
      ByteBuffer buffer = ByteBuffer.wrap(payload);
      while (buffer.hasRemaining()) {
        int id = buffer.getShort() & 0xffff;
        long value = buffer.getInt() & 0xffffffffL;
        if (id == Http2.SETTINGS_ENABLE_PUSH && value != 0) {
          throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "a server enabling push");
        }
        if (id == Http2.SETTINGS_INITIAL_WINDOW_SIZE && value > Integer.MAX_VALUE) {
          throw new Http2.ConnectionError(
              Http2.FLOW_CONTROL_ERROR, "an initial window of " + value);
        }
        if (id == Http2.SETTINGS_MAX_FRAME_SIZE
            && (value < Http2.MAX_FRAME_SIZE || value > 0xffffff)) {
          throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "a max frame size of " + value);
        }
      }
      settingsSeen = true;
      writeFrame(Http2.SETTINGS, Http2.FLAG_ACK, 0, new byte[0]);
    }

    private void ping(int flags, int stream, byte[] payload)
        throws IOException, Http2.ConnectionError {
      if (stream != 0) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "PING on stream " + stream);
      }
      requireLength(payload, 8, "PING");
      if ((flags & Http2.FLAG_ACK) == 0) {
        writeFrame(Http2.PING, Http2.FLAG_ACK, 0, payload);
      }
    }

    private void windowUpdate(byte[] payload) throws Http2.ConnectionError {
      requireLength(payload, 4, "WINDOW_UPDATE");
      int increment = ByteBuffer.wrap(payload).getInt() & 0x7fffffff;
      if (increment == 0) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "a WINDOW_UPDATE of 0");
      }
      // This client sends nothing after its request, so the credit it is given is not tracked.
    }

    private GrpcStatus goAway(int stream, byte[] payload) throws Http2.ConnectionError {
      if (stream != 0) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "GOAWAY on stream " + stream);
      }
      if (payload.length < 8) {
        throw new Http2.ConnectionError(Http2.FRAME_SIZE_ERROR, "GOAWAY of " + payload.length);
      }
      ByteBuffer buffer = ByteBuffer.wrap(payload);
      int lastStream = buffer.getInt() & 0x7fffffff;
      long code = buffer.getInt() & 0xffffffffL;
      if (lastStream < STREAM) {
        return GrpcStatus.of(
            Code.UNAVAILABLE, "the server is going away (HTTP/2 error " + code + ")");
      }
      return null;
    }

    private GrpcStatus headers(int flags, int stream, byte[] payload)
        throws IOException, Http2.ConnectionError {
      requireOurStream(stream, "HEADERS");
      int offset = 0;
      int end = payload.length;
      if ((flags & Http2.FLAG_PADDED) != 0) {
        if (payload.length < 1) {
          throw new Http2.ConnectionError(Http2.FRAME_SIZE_ERROR, "a padded HEADERS frame of 0");
        }
        offset = 1;
        end -= payload[0] & 0xff;
      }
      if ((flags & Http2.FLAG_PRIORITY) != 0) {
        offset += 5;
      }
      if (end < offset) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "HEADERS padding beyond the frame");
      }
      headerBlock = new ByteArrayOutputStream();
      headerBlockEndsStream = (flags & Http2.FLAG_END_STREAM) != 0;
      return fragment(flags, payload, offset, end);
    }

    private GrpcStatus continuation(int flags, byte[] payload)
        throws IOException, Http2.ConnectionError {
      if (headerBlock == null) {
        throw new Http2.ConnectionError(
            Http2.PROTOCOL_ERROR, "CONTINUATION outside a header block");
      }
      return fragment(flags, payload, 0, payload.length);
    }

    private GrpcStatus fragment(int flags, byte[] payload, int from, int to)
        throws IOException, Http2.ConnectionError {
      headerBlock.write(payload, from, to - from);
      if (headerBlock.size() > MAX_HEADER_LIST) {
        throw new Http2.ConnectionError(
            Http2.ENHANCE_YOUR_CALM, "a header block larger than " + MAX_HEADER_LIST);
      }
      if ((flags & Http2.FLAG_END_HEADERS) == 0) {
        return null;
      }
      byte[] block = headerBlock.toByteArray();
      headerBlock = null;
      List<Field> fields;
      try {
        fields = decoder.decode(block);
      } catch (HpackException e) {
        throw new Http2.ConnectionError(Http2.COMPRESSION_ERROR, e.getMessage(), e);
      }
      return headerList(fields, headerBlockEndsStream);
    }

    private GrpcStatus headerList(List<Field> fields, boolean endStream)
        throws Http2.ConnectionError {
      if (!responseHeadersSeen) {
        String status = value(fields, ":status");
        if (status == null || !status.matches("[0-9]{3}")) {
          throw new Http2.ConnectionError(
              Http2.PROTOCOL_ERROR, "a response without a valid :status");
        }
        int code = Integer.parseInt(status);
        if (code >= 100 && code < 200) {
          // An informational response, such as 103, precedes the final one (RFC 9113 section
          // 8.1). 101 has no meaning in HTTP/2, and an informational response cannot end a stream.
          if (code == 101 || endStream) {
            throw new Http2.ConnectionError(
                Http2.PROTOCOL_ERROR, "an informational response " + code + " that is not allowed");
          }
          return null;
        }
        responseHeadersSeen = true;
        httpStatus = code;
        String contentType = value(fields, "content-type");
        grpcResponse =
            httpStatus == 200 && contentType != null && contentType.startsWith("application/grpc");
        return endStream ? status(fields) : null;
      }
      if (!endStream) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "trailers without END_STREAM");
      }
      GrpcStatus status = status(fields);
      if (status.isOk() && (prefixFilled > 0 || body != null)) {
        return GrpcStatus.of(Code.INTERNAL, "the stream ended inside a message");
      }
      return status;
    }

    /** The status a trailer list gives: grpc-status first, the HTTP status without it. */
    private GrpcStatus status(List<Field> fields) {
      String grpcStatus = value(fields, "grpc-status");
      if (grpcStatus != null) {
        Code code;
        try {
          code = Code.of(Integer.parseInt(grpcStatus.trim()));
        } catch (NumberFormatException e) {
          code = Code.UNKNOWN;
        }
        String message = value(fields, "grpc-message");
        return GrpcStatus.of(code, message == null ? "" : percentDecode(message));
      }
      if (httpStatus != 200) {
        return GrpcStatus.ofHttp(httpStatus);
      }
      return GrpcStatus.of(Code.UNKNOWN, "a response without grpc-status");
    }

    private GrpcStatus data(int flags, int stream, byte[] payload)
        throws IOException, Http2.ConnectionError {
      requireOurStream(stream, "DATA");
      if (!responseHeadersSeen) {
        throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "DATA before the response headers");
      }
      connectionWindow -= payload.length;
      streamWindow -= payload.length;
      if (connectionWindow < 0 || streamWindow < 0) {
        throw new Http2.ConnectionError(Http2.FLOW_CONTROL_ERROR, "DATA beyond the window");
      }
      int offset = 0;
      int end = payload.length;
      if ((flags & Http2.FLAG_PADDED) != 0) {
        if (payload.length < 1 || (payload[0] & 0xff) >= payload.length) {
          throw new Http2.ConnectionError(Http2.PROTOCOL_ERROR, "DATA padding beyond the frame");
        }
        offset = 1;
        end -= payload[0] & 0xff;
      }
      if (grpcResponse) {
        GrpcStatus failed = deliver(payload, offset, end);
        if (failed != null) {
          return failed;
        }
      }
      if ((flags & Http2.FLAG_END_STREAM) != 0) {
        // Without trailers there is no grpc-status: a non-gRPC response (an HTTP error with a
        // body) is mapped from its HTTP status, a gRPC one is cut short.
        if (httpStatus != 200) {
          return GrpcStatus.ofHttp(httpStatus);
        }
        return grpcResponse
            ? GrpcStatus.of(Code.INTERNAL, "the stream ended without trailers")
            : GrpcStatus.of(Code.UNKNOWN, "a response that is not gRPC, without grpc-status");
      }
      credit(payload.length);
      return null;
    }

    /**
     * Adds a DATA frame's octets to the message being assembled, handing each message to the
     * consumer as it completes, so that every octet is copied a fixed number of times. Returns a
     * status if a message is refused, or if the call is closed from the consumer.
     */
    private GrpcStatus deliver(byte[] data, int from, int to) {
      int position = from;
      while (position < to) {
        if (body == null) {
          int n = Math.min(prefix.length - prefixFilled, to - position);
          System.arraycopy(data, position, prefix, prefixFilled, n);
          prefixFilled += n;
          position += n;
          if (prefixFilled < prefix.length) {
            break;
          }
          int length = ByteBuffer.wrap(prefix, 1, 4).getInt();
          if (prefix[0] != 0) {
            return GrpcStatus.of(Code.INTERNAL, "a compressed message, though none was offered");
          }
          if (length < 0 || length > MAX_MESSAGE) {
            return GrpcStatus.of(
                Code.RESOURCE_EXHAUSTED, "a message larger than " + MAX_MESSAGE + " octets");
          }
          body = new byte[length];
          bodyFilled = 0;
        }
        int n = Math.min(body.length - bodyFilled, to - position);
        System.arraycopy(data, position, body, bodyFilled, n);
        bodyFilled += n;
        position += n;
        if (bodyFilled == body.length && complete()) {
          return GrpcStatus.of(Code.CANCELLED, "the call was closed");
        }
      }
      // A message of no octets completes with its prefix, which may end the frame.
      if (body != null && body.length == 0 && complete()) {
        return GrpcStatus.of(Code.CANCELLED, "the call was closed");
      }
      return null;
    }

    /** Hands the assembled message to the consumer; returns whether the call was closed. */
    private boolean complete() {
      byte[] message = body;
      body = null;
      prefixFilled = 0;
      onMessage.accept(message);
      return closed;
    }

    /** Returns flow-control credit for data consumed, once half a window has been. */
    private void credit(int length) throws IOException {
      connectionConsumed += length;
      streamConsumed += length;
      if (connectionConsumed >= WINDOW / 2) {
        writeFrame(Http2.WINDOW_UPDATE, 0, 0, int32(connectionConsumed));
        connectionWindow += connectionConsumed;
        connectionConsumed = 0;
      }
      if (streamConsumed >= WINDOW / 2) {
        writeFrame(Http2.WINDOW_UPDATE, 0, STREAM, int32(streamConsumed));
        streamWindow += streamConsumed;
        streamConsumed = 0;
      }
    }

    private void requireOurStream(int stream, String type) throws Http2.ConnectionError {
      if (stream != STREAM) {
        throw new Http2.ConnectionError(
            Http2.PROTOCOL_ERROR,
            type + " on stream " + stream + ", which this client never opened");
      }
    }

    private void requireLength(byte[] payload, int length, String type)
        throws Http2.ConnectionError {
      if (payload.length != length) {
        throw new Http2.ConnectionError(
            Http2.FRAME_SIZE_ERROR, type + " of " + payload.length + " octets");
      }
    }
  }

  private static String value(List<Field> fields, String name) {
    for (Field field : fields) {
      if (field.name().equals(name)) {
        return field.value();
      }
    }
    return null;
  }

  /** Decodes grpc-message's percent-encoding (UTF-8); a malformed escape is kept as it is. */
  static String percentDecode(String value) {
    if (value.indexOf('%') < 0) {
      return value;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '%' && i + 2 < value.length()) {
        int hi = Character.digit(value.charAt(i + 1), 16);
        int lo = Character.digit(value.charAt(i + 2), 16);
        if (hi >= 0 && lo >= 0) {
          out.write(hi * 16 + lo);
          i += 2;
          continue;
        }
      }
      out.write(c);
    }
    return out.toString(StandardCharsets.UTF_8);
  }
}
