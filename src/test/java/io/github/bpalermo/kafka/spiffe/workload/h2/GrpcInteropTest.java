package io.github.bpalermo.kafka.spiffe.workload.h2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcStatus.Code;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The client against grpc-java's server on a Unix domain socket: our HTTP/2 meets theirs. */
class GrpcInteropTest {

  private static final MethodDescriptor.Marshaller<byte[]> BYTES =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
          return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
          try {
            return stream.readAllBytes();
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }
      };

  private static final MethodDescriptor<byte[], byte[]> METHOD =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("test.Streams/Fetch")
          .setRequestMarshaller(BYTES)
          .setResponseMarshaller(BYTES)
          .build();

  private static final Metadata.Key<String> WORKLOAD =
      Metadata.Key.of("workload.spiffe.io", Metadata.ASCII_STRING_MARSHALLER);

  private Path directory;
  private Path socket;
  private EventLoopGroup group;
  private Server server;
  private final AtomicReference<String> header = new AtomicReference<>();

  @BeforeEach
  void socket() throws IOException {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the test server needs Netty's epoll");
    // Unix socket paths are short; Bazel's test directories are not.
    directory = Files.createTempDirectory(Path.of("/tmp"), "h2");
    socket = directory.resolve("s");
  }

  @AfterEach
  void stop() throws Exception {
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    if (group != null) {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }
    if (directory != null) {
      Files.deleteIfExists(socket);
      Files.deleteIfExists(directory);
    }
  }

  private void serve(BiConsumer<byte[], StreamObserver<byte[]>> behaviour) throws IOException {
    group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    ServerServiceDefinition service =
        ServerServiceDefinition.builder("test.Streams")
            .addMethod(METHOD, ServerCalls.asyncServerStreamingCall(behaviour::accept))
            .build();
    ServerInterceptor capture =
        new ServerInterceptor() {
          @Override
          public <Q, R> ServerCall.Listener<Q> interceptCall(
              ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
            header.set(headers.get(WORKLOAD));
            return next.startCall(call, headers);
          }
        };
    server =
        NettyServerBuilder.forAddress(new DomainSocketAddress(socket.toString()))
            .channelType(EpollServerDomainSocketChannel.class)
            .bossEventLoopGroup(group)
            .workerEventLoopGroup(group)
            .addService(ServerInterceptors.intercept(service, capture))
            .build()
            .start();
  }

  private record Result(GrpcStatus status, List<byte[]> messages) {}

  private Result call() throws IOException {
    List<byte[]> messages = new ArrayList<>();
    GrpcStatus status =
        GrpcCall.connect(socket)
            .run(
                "/test.Streams/Fetch",
                Map.of("workload.spiffe.io", "true"),
                new byte[0],
                messages::add);
    return new Result(status, messages);
  }

  @Test
  void streamsMessagesOfEverySizeThenOk() throws IOException {
    byte[] big = new byte[5 << 20];
    Arrays.fill(big, (byte) 3);
    serve(
        (request, responses) -> {
          responses.onNext(new byte[] {1});
          responses.onNext(new byte[0]);
          responses.onNext(big);
          for (int i = 0; i < 100; i++) {
            responses.onNext(new byte[64 << 10]);
          }
          responses.onCompleted();
        });
    Result result = call();
    assertTrue(result.status().isOk(), result.status().toString());
    assertEquals("true", header.get());
    assertEquals(103, result.messages().size());
    assertArrayEquals(new byte[] {1}, result.messages().get(0));
    assertArrayEquals(big, result.messages().get(2));
  }

  @Test
  void anErrorAfterMessagesCarriesItsStatusAndMessage() throws IOException {
    serve(
        (request, responses) -> {
          responses.onNext(new byte[] {1});
          responses.onError(
              Status.PERMISSION_DENIED.withDescription("no entry for café").asRuntimeException());
        });
    Result result = call();
    assertEquals(Code.PERMISSION_DENIED, result.status().code());
    assertEquals("no entry for café", result.status().message());
    assertEquals(1, result.messages().size());
  }

  @Test
  void aTrailersOnlyErrorCarriesItsStatus() throws IOException {
    serve(
        (request, responses) ->
            responses.onError(Status.INVALID_ARGUMENT.withDescription("bad").asRuntimeException()));
    Result result = call();
    assertEquals(Code.INVALID_ARGUMENT, result.status().code());
    assertEquals("bad", result.status().message());
  }

  @Test
  void anUnknownMethodIsUnimplemented() throws IOException {
    serve((request, responses) -> responses.onCompleted());
    GrpcStatus status =
        GrpcCall.connect(socket).run("/test.Streams/Nope", Map.of(), new byte[0], m -> {});
    assertEquals(Code.UNIMPLEMENTED, status.code());
  }

  @Test
  void aServerThatShutsDownEndsTheCall() throws Exception {
    CountDownLatch streaming = new CountDownLatch(1);
    serve(
        (request, responses) -> {
          responses.onNext(new byte[] {1});
          streaming.countDown();
        });
    AtomicReference<Result> result = new AtomicReference<>();
    Thread caller =
        new Thread(
            () -> {
              try {
                result.set(call());
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            });
    caller.start();
    assertTrue(streaming.await(5, TimeUnit.SECONDS));
    server.shutdownNow();
    caller.join(5000);
    Code code = result.get().status().code();
    assertTrue(
        code == Code.UNAVAILABLE || code == Code.CANCELLED, result.get().status().toString());
  }

  @Test
  void connectingToNoServerFails() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IOException.class, () -> GrpcCall.connect(socket));
  }
}
