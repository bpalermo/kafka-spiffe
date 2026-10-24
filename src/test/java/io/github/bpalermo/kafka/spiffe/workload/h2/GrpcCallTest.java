package io.github.bpalermo.kafka.spiffe.workload.h2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcStatus.Code;
import io.github.bpalermo.kafka.spiffe.workload.h2.Hpack.Field;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The client against a scripted server: frames written byte for byte, read back in pieces of any
 * size, and what the client writes in return.
 */
class GrpcCallTest {

  /** A channel that serves scripted octets in reads of at most {@code chunk} octets. */
  private static final class ScriptedChannel implements ByteChannel {
    private final byte[] script;
    private final int chunk;
    private final ByteArrayOutputStream written = new ByteArrayOutputStream();
    private int position;
    private boolean open = true;

    ScriptedChannel(byte[] script, int chunk) {
      this.script = script;
      this.chunk = chunk;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      if (position == script.length) {
        return -1;
      }
      int n = Math.min(Math.min(chunk, dst.remaining()), script.length - position);
      dst.put(script, position, n);
      position += n;
      return n;
    }

    @Override
    public int write(ByteBuffer src) {
      int n = src.remaining();
      byte[] bytes = new byte[n];
      src.get(bytes);
      written.write(bytes, 0, n);
      return n;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }

    /** The frames the client wrote after its preface, as {type, flags, stream, payload}. */
    List<Object[]> frames() {
      byte[] all = written.toByteArray();
      List<Object[]> frames = new ArrayList<>();
      int i = Http2.PREFACE.length;
      while (i + 9 <= all.length) {
        int length = ((all[i] & 0xff) << 16) | ((all[i + 1] & 0xff) << 8) | (all[i + 2] & 0xff);
        int stream = ByteBuffer.wrap(all, i + 5, 4).getInt();
        frames.add(
            new Object[] {
              all[i + 3] & 0xff,
              all[i + 4] & 0xff,
              stream,
              Arrays.copyOfRange(all, i + 9, i + 9 + length)
            });
        i += 9 + length;
      }
      return frames;
    }

    byte[] preface() {
      return Arrays.copyOf(written.toByteArray(), Http2.PREFACE.length);
    }
  }

  /** Builds what a server sends. */
  private static final class Server {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    Server frame(int type, int flags, int stream, byte[] payload) {
      out.write(payload.length >>> 16);
      out.write(payload.length >>> 8);
      out.write(payload.length);
      out.write(type);
      out.write(flags);
      out.writeBytes(ByteBuffer.allocate(4).putInt(stream).array());
      out.writeBytes(payload);
      return this;
    }

    Server settings() {
      return frame(Http2.SETTINGS, 0, 0, new byte[0]);
    }

    Server headers(int flags, String... nameValues) {
      List<Field> fields = new ArrayList<>();
      for (int i = 0; i < nameValues.length; i += 2) {
        fields.add(new Field(nameValues[i], nameValues[i + 1]));
      }
      return frame(Http2.HEADERS, flags | Http2.FLAG_END_HEADERS, 1, Hpack.encode(fields));
    }

    Server responseHeaders() {
      return headers(0, ":status", "200", "content-type", "application/grpc");
    }

    Server ok() {
      return headers(Http2.FLAG_END_STREAM, "grpc-status", "0");
    }

    Server message(byte[] message) {
      return frame(Http2.DATA, 0, 1, grpc(message));
    }

    byte[] bytes() {
      return out.toByteArray();
    }
  }

  private static byte[] grpc(byte[] message) {
    return ByteBuffer.allocate(5 + message.length)
        .put((byte) 0)
        .putInt(message.length)
        .put(message)
        .array();
  }

  private record Result(GrpcStatus status, List<byte[]> messages, ScriptedChannel channel) {}

  private static Result run(byte[] script, int chunk) {
    ScriptedChannel channel = new ScriptedChannel(script, chunk);
    List<byte[]> messages = new ArrayList<>();
    GrpcStatus status =
        new GrpcCall(channel)
            .run("/a.B/C", Map.of("workload.spiffe.io", "true"), new byte[0], messages::add);
    return new Result(status, messages, channel);
  }

  private static Result run(Server server) {
    return run(server.bytes(), Integer.MAX_VALUE);
  }

  private static int goAwayCode(Result result) {
    for (Object[] frame : result.channel().frames()) {
      if ((int) frame[0] == Http2.GOAWAY) {
        return ByteBuffer.wrap((byte[]) frame[3], 4, 4).getInt();
      }
    }
    throw new AssertionError("no GOAWAY");
  }

  @Test
  void sendsThePrefaceSettingsAndTheRequest() throws HpackException {
    Result result = run(new Server().settings().responseHeaders().ok());
    assertArrayEquals(Http2.PREFACE, result.channel().preface());
    List<Object[]> frames = result.channel().frames();
    assertEquals(Http2.SETTINGS, frames.get(0)[0]);
    assertArrayEquals(
        ByteBuffer.allocate(12)
            .putShort((short) 2)
            .putInt(0)
            .putShort((short) 4)
            .putInt(1 << 20)
            .array(),
        (byte[]) frames.get(0)[3]);
    assertEquals(Http2.WINDOW_UPDATE, frames.get(1)[0]);
    assertEquals(Http2.HEADERS, frames.get(2)[0]);
    assertEquals(Http2.FLAG_END_HEADERS, frames.get(2)[1]);
    List<Field> request = new Hpack.Decoder(4096, 1 << 16).decode((byte[]) frames.get(2)[3]);
    assertTrue(request.contains(new Field(":path", "/a.B/C")));
    assertTrue(request.contains(new Field("te", "trailers")));
    assertTrue(request.contains(new Field("workload.spiffe.io", "true")));
    assertEquals(Http2.DATA, frames.get(3)[0]);
    assertEquals(Http2.FLAG_END_STREAM, frames.get(3)[1]);
    assertArrayEquals(new byte[5], (byte[]) frames.get(3)[3]);
    assertEquals(Http2.SETTINGS, frames.get(4)[0]);
    assertEquals(Http2.FLAG_ACK, frames.get(4)[1]);
  }

  @Test
  void deliversMessagesThenTheStatus() {
    Server server =
        new Server()
            .settings()
            .responseHeaders()
            .message(new byte[] {1, 2, 3})
            .message(new byte[0])
            .headers(Http2.FLAG_END_STREAM, "grpc-status", "0");
    Result result = run(server);
    assertTrue(result.status().isOk(), result.status().toString());
    assertEquals(2, result.messages().size());
    assertArrayEquals(new byte[] {1, 2, 3}, result.messages().get(0));
  }

  @Test
  void readsCorrectlyWhateverTheReadsSplit() {
    byte[] big = new byte[40_000];
    Arrays.fill(big, (byte) 7);
    byte[] grpc = grpc(big);
    Server server = new Server().settings().responseHeaders();
    for (int offset = 0; offset < grpc.length; offset += Http2.MAX_FRAME_SIZE) {
      server.frame(
          Http2.DATA,
          0,
          1,
          Arrays.copyOfRange(grpc, offset, Math.min(grpc.length, offset + Http2.MAX_FRAME_SIZE)));
    }
    server.message(new byte[] {9}).ok();
    byte[] script = server.bytes();
    for (int chunk : new int[] {1, 2, 3, 5, 8, 9, 10, 4096, 16_393, Integer.MAX_VALUE}) {
      Result result = run(script, chunk);
      assertTrue(result.status().isOk(), chunk + ": " + result.status());
      assertEquals(2, result.messages().size(), "chunk " + chunk);
      assertArrayEquals(big, result.messages().get(0));
    }
  }

  @Test
  void handlesPaddingContinuationAndPriority() {
    byte[] block =
        Hpack.encode(
            List.of(new Field(":status", "200"), new Field("content-type", "application/grpc")));
    byte[] padded = new byte[1 + 5 + 3 + block.length / 2 + 3];
    ByteBuffer headers = ByteBuffer.wrap(padded);
    headers.put((byte) 3).put(new byte[5]).put(block, 0, block.length / 2);
    Server server =
        new Server()
            .settings()
            .frame(
                Http2.HEADERS,
                Http2.FLAG_PADDED | Http2.FLAG_PRIORITY,
                1,
                Arrays.copyOf(padded, 1 + 5 + block.length / 2 + 3))
            .frame(
                Http2.CONTINUATION,
                Http2.FLAG_END_HEADERS,
                1,
                Arrays.copyOfRange(block, block.length / 2, block.length))
            .frame(Http2.PRIORITY, 0, 1, new byte[5])
            .frame(0x42, 0, 0, new byte[] {1, 2, 3});
    byte[] data = grpc(new byte[] {5});
    byte[] paddedData = new byte[1 + data.length + 4];
    paddedData[0] = 4;
    System.arraycopy(data, 0, paddedData, 1, data.length);
    server.frame(Http2.DATA, Http2.FLAG_PADDED, 1, paddedData).ok();
    Result result = run(server);
    assertTrue(result.status().isOk(), result.status().toString());
    assertArrayEquals(new byte[] {5}, result.messages().get(0));
  }

  @Test
  void answersPingsAndReturnsFlowControlCredit() {
    byte[] chunk = new byte[Http2.MAX_FRAME_SIZE];
    Server server = new Server().settings().responseHeaders();
    server.frame(Http2.PING, 0, 0, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    // The message prefix announces 40 frames' worth; more than half the 1 MiB window flows.
    int total = 40 * Http2.MAX_FRAME_SIZE;
    server.frame(Http2.DATA, 0, 1, ByteBuffer.allocate(5).put((byte) 0).putInt(total).array());
    for (int i = 0; i < 40; i++) {
      server.frame(Http2.DATA, 0, 1, chunk);
    }
    server.ok();
    Result result = run(server);
    assertTrue(result.status().isOk(), result.status().toString());
    boolean pong = false;
    boolean connectionCredit = false;
    boolean streamCredit = false;
    for (Object[] frame : result.channel().frames()) {
      if ((int) frame[0] == Http2.PING) {
        pong =
            (int) frame[1] == Http2.FLAG_ACK
                && Arrays.equals((byte[]) frame[3], new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
      }
      if ((int) frame[0] == Http2.WINDOW_UPDATE
          && (int) frame[2] == 0
          && ByteBuffer.wrap((byte[]) frame[3]).getInt() != (1 << 20) - 65_535) {
        connectionCredit = true;
      }
      if ((int) frame[0] == Http2.WINDOW_UPDATE && (int) frame[2] == 1) {
        streamCredit = true;
      }
    }
    assertTrue(pong, "no PING ack");
    assertTrue(connectionCredit, "no connection WINDOW_UPDATE");
    assertTrue(streamCredit, "no stream WINDOW_UPDATE");
  }

  @Test
  void anExplicitGrpcStatusWinsOverTheHttpStatus() {
    Result result =
        run(
            new Server()
                .settings()
                .headers(
                    Http2.FLAG_END_STREAM,
                    ":status",
                    "503",
                    "grpc-status",
                    "7",
                    "grpc-message",
                    "no entry for caf%C3%A9"));
    assertEquals(Code.PERMISSION_DENIED, result.status().code());
    assertEquals("no entry for café", result.status().message());
  }

  @Test
  void withoutGrpcStatusTheHttpStatusDecides() {
    for (Object[] pair :
        new Object[][] {
          {"400", Code.INTERNAL},
          {"401", Code.UNAUTHENTICATED},
          {"403", Code.PERMISSION_DENIED},
          {"404", Code.UNIMPLEMENTED},
          {"429", Code.UNAVAILABLE},
          {"502", Code.UNAVAILABLE},
          {"503", Code.UNAVAILABLE},
          {"504", Code.UNAVAILABLE},
          {"418", Code.UNKNOWN},
          {"200", Code.UNKNOWN},
        }) {
      Result result =
          run(new Server().settings().headers(Http2.FLAG_END_STREAM, ":status", (String) pair[0]));
      assertEquals(pair[1], result.status().code(), (String) pair[0]);
    }
  }

  @Test
  void aNon200ResponsesBodyIsNotDelivered() {
    Result result =
        run(
            new Server()
                .settings()
                .headers(0, ":status", "503", "content-type", "text/plain")
                .frame(Http2.DATA, 0, 1, new byte[] {'o', 'v', 'e', 'r'})
                .headers(Http2.FLAG_END_STREAM, "x", "y"));
    assertEquals(Code.UNAVAILABLE, result.status().code());
    assertTrue(result.messages().isEmpty());
  }

  @Test
  void mapsResetCodesAsGrpcDoes() {
    for (Object[] pair :
        new Object[][] {
          {Http2.REFUSED_STREAM, Code.UNAVAILABLE},
          {Http2.CANCEL, Code.CANCELLED},
          {Http2.ENHANCE_YOUR_CALM, Code.RESOURCE_EXHAUSTED},
          {Http2.INADEQUATE_SECURITY, Code.PERMISSION_DENIED},
          {Http2.NO_ERROR, Code.INTERNAL},
          {Http2.PROTOCOL_ERROR, Code.INTERNAL},
          {Http2.INTERNAL_ERROR, Code.INTERNAL},
        }) {
      Result result =
          run(
              new Server()
                  .settings()
                  .responseHeaders()
                  .frame(
                      Http2.RST_STREAM,
                      0,
                      1,
                      ByteBuffer.allocate(4).putInt((int) pair[0]).array()));
      assertEquals(pair[1], result.status().code(), "HTTP/2 error " + pair[0]);
    }
  }

  @Test
  void aGoawayBelowTheStreamIsUnavailableOneCoveringItLetsItFinish() {
    Result refused =
        run(
            new Server()
                .settings()
                .frame(Http2.GOAWAY, 0, 0, ByteBuffer.allocate(8).putInt(0).putInt(0).array()));
    assertEquals(Code.UNAVAILABLE, refused.status().code());

    Result finished =
        run(
            new Server()
                .settings()
                .responseHeaders()
                .frame(Http2.GOAWAY, 0, 0, ByteBuffer.allocate(8).putInt(1).putInt(0).array())
                .message(new byte[] {1})
                .ok());
    assertTrue(finished.status().isOk(), finished.status().toString());
    assertEquals(1, finished.messages().size());
  }

  @Test
  void aConnectionThatClosesBeforeTheTrailersIsUnavailable() {
    Result result = run(new Server().settings().responseHeaders().message(new byte[] {1}));
    assertEquals(Code.UNAVAILABLE, result.status().code());
    assertEquals(1, result.messages().size());
  }

  @Test
  void sendsTheErrorCodeEachViolationRequires() {
    Object[][] cases = {
      {new Server().frame(Http2.PING, 0, 0, new byte[8]), Http2.PROTOCOL_ERROR},
      {
        new Server().settings().frame(Http2.DATA, 0, 1, new byte[Http2.MAX_FRAME_SIZE + 1]),
        Http2.FRAME_SIZE_ERROR
      },
      {new Server().settings().frame(Http2.PING, 0, 0, new byte[7]), Http2.FRAME_SIZE_ERROR},
      {new Server().frame(Http2.SETTINGS, 0, 0, new byte[5]), Http2.FRAME_SIZE_ERROR},
      {
        new Server().settings().frame(Http2.WINDOW_UPDATE, 0, 0, new byte[3]),
        Http2.FRAME_SIZE_ERROR
      },
      {
        new Server()
            .settings()
            .frame(Http2.HEADERS, Http2.FLAG_END_HEADERS, 1, new byte[] {(byte) 0x80}),
        Http2.COMPRESSION_ERROR
      },
      {new Server().settings().frame(Http2.WINDOW_UPDATE, 0, 0, new byte[4]), Http2.PROTOCOL_ERROR},
      {
        new Server().settings().frame(Http2.PUSH_PROMISE, Http2.FLAG_END_HEADERS, 1, new byte[4]),
        Http2.PROTOCOL_ERROR
      },
      {
        new Server()
            .settings()
            .frame(Http2.HEADERS, Http2.FLAG_END_HEADERS, 3, Hpack.encode(List.of())),
        Http2.PROTOCOL_ERROR
      },
      {
        new Server()
            .settings()
            .frame(Http2.HEADERS, 0, 1, Hpack.encode(List.of()))
            .frame(Http2.PING, 0, 0, new byte[8]),
        Http2.PROTOCOL_ERROR
      },
      {
        new Server().settings().frame(Http2.CONTINUATION, Http2.FLAG_END_HEADERS, 1, new byte[0]),
        Http2.PROTOCOL_ERROR
      },
      {new Server().settings().frame(Http2.DATA, 0, 1, new byte[5]), Http2.PROTOCOL_ERROR},
      {
        new Server()
            .frame(
                Http2.SETTINGS, 0, 0, ByteBuffer.allocate(6).putShort((short) 2).putInt(1).array()),
        Http2.PROTOCOL_ERROR
      },
      {
        new Server()
            .frame(
                Http2.SETTINGS,
                0,
                0,
                ByteBuffer.allocate(6).putShort((short) 4).putInt(-1).array()),
        Http2.FLOW_CONTROL_ERROR
      },
    };
    for (int i = 0; i < cases.length; i++) {
      Result result = run((Server) cases[i][0]);
      assertEquals(Code.INTERNAL, result.status().code(), "case " + i + ": " + result.status());
      assertEquals(cases[i][1], goAwayCode(result), "case " + i);
    }
  }

  @Test
  void refusesCompressedAndOversizedMessages() {
    Result compressed =
        run(
            new Server()
                .settings()
                .responseHeaders()
                .frame(
                    Http2.DATA,
                    0,
                    1,
                    ByteBuffer.allocate(6).put((byte) 1).putInt(1).put((byte) 0).array()));
    assertEquals(Code.INTERNAL, compressed.status().code());

    Result oversized =
        run(
            new Server()
                .settings()
                .responseHeaders()
                .frame(
                    Http2.DATA,
                    0,
                    1,
                    ByteBuffer.allocate(5).put((byte) 0).putInt(GrpcCall.MAX_MESSAGE + 1).array()));
    assertEquals(Code.RESOURCE_EXHAUSTED, oversized.status().code());
  }

  @Test
  void trailersWithoutEndStreamAndDataEndingTheStreamAreErrors() {
    Result noEnd = run(new Server().settings().responseHeaders().headers(0, "grpc-status", "0"));
    assertEquals(Code.INTERNAL, noEnd.status().code());

    Result endOnData =
        run(
            new Server()
                .settings()
                .responseHeaders()
                .frame(Http2.DATA, Http2.FLAG_END_STREAM, 1, grpc(new byte[] {1})));
    assertEquals(Code.INTERNAL, endOnData.status().code());
    assertEquals(1, endOnData.messages().size());
  }

  @Test
  void decodesPercentEncodedMessages() {
    assertEquals("a b", GrpcCall.percentDecode("a%20b"));
    assertEquals("100%", GrpcCall.percentDecode("100%"));
    assertEquals("%zz", GrpcCall.percentDecode("%zz"));
  }

  @Test
  void closingTheCallEndsItAsCancelled() throws Exception {
    ByteChannel blocking =
        new ByteChannel() {
          private volatile boolean open = true;

          @Override
          public synchronized int read(ByteBuffer dst) throws IOException {
            while (open) {
              try {
                wait();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
            }
            throw new ClosedChannelException();
          }

          @Override
          public int write(ByteBuffer src) {
            int n = src.remaining();
            src.position(src.limit());
            return n;
          }

          @Override
          public boolean isOpen() {
            return open;
          }

          @Override
          public synchronized void close() {
            open = false;
            notifyAll();
          }
        };
    GrpcCall call = new GrpcCall(blocking);
    GrpcStatus[] status = new GrpcStatus[1];
    Thread runner =
        new Thread(() -> status[0] = call.run("/a.B/C", Map.of(), new byte[0], m -> {}));
    runner.start();
    Thread.sleep(100);
    call.close();
    runner.join(5000);
    assertEquals(Code.CANCELLED, status[0].code());
  }

  @Test
  void skipsInformationalResponsesBeforeTheFinalOne() {
    Result result =
        run(
            new Server()
                .settings()
                .headers(0, ":status", "103", "link", "</a>; rel=preload")
                .responseHeaders()
                .message(new byte[] {4})
                .ok());
    assertTrue(result.status().isOk(), result.status().toString());
    assertEquals(1, result.messages().size());

    Result switching = run(new Server().settings().headers(0, ":status", "101"));
    assertEquals(Http2.PROTOCOL_ERROR, goAwayCode(switching));
    Result ending = run(new Server().settings().headers(Http2.FLAG_END_STREAM, ":status", "103"));
    assertEquals(Http2.PROTOCOL_ERROR, goAwayCode(ending));
  }

  @Test
  void anHttpErrorWithABodyMapsFromItsStatus() {
    for (Object[] pair :
        new Object[][] {
          {"503", Code.UNAVAILABLE}, {"401", Code.UNAUTHENTICATED}, {"200", Code.UNKNOWN}
        }) {
      Result result =
          run(
              new Server()
                  .settings()
                  .headers(0, ":status", (String) pair[0], "content-type", "text/plain")
                  .frame(Http2.DATA, Http2.FLAG_END_STREAM, 1, new byte[] {'n', 'o'}));
      assertEquals(pair[1], result.status().code(), (String) pair[0]);
      assertTrue(result.messages().isEmpty());
    }
  }

  @Test
  void deliversEmptyMessagesThatEndAFrame() {
    Server server = new Server().settings().responseHeaders();
    server.frame(Http2.DATA, 0, 1, grpc(new byte[0]));
    byte[] two = new byte[10];
    two[4] = 0;
    server.frame(Http2.DATA, 0, 1, two);
    server.ok();
    for (int chunk : new int[] {1, 3, Integer.MAX_VALUE}) {
      Result result = run(server.bytes(), chunk);
      assertTrue(result.status().isOk(), result.status().toString());
      assertEquals(3, result.messages().size(), "chunk " + chunk);
    }
  }

  @Test
  void assemblesTheLargestMessageInLinearTime() {
    byte[] grpc = grpc(new byte[GrpcCall.MAX_MESSAGE]);
    Server server = new Server().settings().responseHeaders();
    for (int offset = 0; offset < grpc.length; offset += Http2.MAX_FRAME_SIZE) {
      server.frame(
          Http2.DATA,
          0,
          1,
          Arrays.copyOfRange(grpc, offset, Math.min(grpc.length, offset + Http2.MAX_FRAME_SIZE)));
    }
    byte[] script = server.ok().bytes();
    Result result =
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
            java.time.Duration.ofSeconds(20), () -> run(script, Integer.MAX_VALUE));
    assertTrue(result.status().isOk(), result.status().toString());
    assertEquals(GrpcCall.MAX_MESSAGE, result.messages().get(0).length);
  }

  @Test
  void closingFromTheConsumerStopsDeliveryAndEndsAsCancelled() {
    byte[] both = new byte[12];
    both[4] = 1;
    both[5] = 7;
    both[10] = 1;
    both[11] = 8;
    byte[] script =
        new Server().settings().responseHeaders().frame(Http2.DATA, 0, 1, both).ok().bytes();
    ScriptedChannel channel = new ScriptedChannel(script, Integer.MAX_VALUE);
    GrpcCall call = new GrpcCall(channel);
    List<byte[]> messages = new ArrayList<>();
    GrpcStatus status =
        call.run(
            "/a.B/C",
            Map.of(),
            new byte[0],
            message -> {
              messages.add(message);
              call.close();
            });
    assertEquals(Code.CANCELLED, status.code());
    assertEquals(1, messages.size());
  }
}
