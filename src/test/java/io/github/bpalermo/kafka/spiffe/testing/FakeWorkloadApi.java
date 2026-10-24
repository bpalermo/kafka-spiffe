package io.github.bpalermo.kafka.spiffe.testing;

import com.google.protobuf.CodedOutputStream;
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
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Workload API for tests: grpc-java on a Unix domain socket, serving {@code FetchX509SVID} like a
 * SPIRE agent does. Every stream gets the newest published response at once and every response
 * published after; a call without the security header is refused with {@code INVALID_ARGUMENT}, as
 * the agent does. It can be told to fail calls, and stopped and started again on the same socket.
 */
public final class FakeWorkloadApi implements AutoCloseable {

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
            throw new UncheckedIOException(e);
          }
        }
      };

  private static final MethodDescriptor<byte[], byte[]> FETCH_X509_SVID =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("SpiffeWorkloadAPI/FetchX509SVID")
          .setRequestMarshaller(BYTES)
          .setResponseMarshaller(BYTES)
          .build();

  private static final Metadata.Key<String> SECURITY_HEADER =
      Metadata.Key.of("workload.spiffe.io", Metadata.ASCII_STRING_MARSHALLER);

  private final Path socket;
  private final List<StreamObserver<byte[]>> streams = new CopyOnWriteArrayList<>();
  private final List<Status> failures = new CopyOnWriteArrayList<>();
  private final AtomicInteger calls = new AtomicInteger();
  private final Object lock = new Object();
  private volatile byte[] latest;
  private volatile Status failAll;
  private EventLoopGroup group;
  private Server server;

  private FakeWorkloadApi(Path socket) {
    this.socket = socket;
  }

  /** Starts serving on {@code socket}. */
  public static FakeWorkloadApi start(Path socket) throws IOException {
    FakeWorkloadApi api = new FakeWorkloadApi(socket);
    api.startServer();
    return api;
  }

  /** The socket served. */
  public Path socket() {
    return socket;
  }

  /** How many calls arrived, refused ones included. */
  public int calls() {
    return calls.get();
  }

  /** Sends {@code response} to every open stream, and to every stream opened later. */
  public void publish(Response response) {
    publish(response.encode());
  }

  /** Sends raw octets as a response message: for malformed responses. */
  public void publish(byte[] message) {
    synchronized (lock) {
      latest = message;
      for (StreamObserver<byte[]> stream : streams) {
        stream.onNext(message);
      }
    }
  }

  /** Makes the next call fail with {@code status}, as a trailers-only response. */
  public void failNext(Status status) {
    failures.add(status);
  }

  /** Makes every call fail with {@code status}, or none if null. */
  public void failAll(Status status) {
    failAll = status;
  }

  /** Ends every open stream with {@code status}. */
  public void endStreams(Status status) {
    synchronized (lock) {
      for (StreamObserver<byte[]> stream : streams) {
        stream.onError(status.asRuntimeException());
      }
      streams.clear();
    }
  }

  /** Stops serving; the socket goes away, open streams end. */
  public void stop() throws InterruptedException {
    streams.clear();
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server = null;
    }
    if (group != null) {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
      group = null;
    }
    try {
      Files.deleteIfExists(socket);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Serves again on the same socket, with the same newest response. */
  public void restart() throws IOException {
    startServer();
  }

  @Override
  public void close() {
    try {
      stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void startServer() throws IOException {
    group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    ServerServiceDefinition service =
        ServerServiceDefinition.builder("SpiffeWorkloadAPI")
            .addMethod(
                FETCH_X509_SVID,
                ServerCalls.asyncServerStreamingCall((request, responses) -> fetch(responses)))
            .build();
    ServerInterceptor securityHeader =
        new ServerInterceptor() {
          @Override
          public <Q, R> ServerCall.Listener<Q> interceptCall(
              ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
            calls.incrementAndGet();
            if (!"true".equals(headers.get(SECURITY_HEADER))) {
              call.close(
                  Status.INVALID_ARGUMENT.withDescription("security header missing from request"),
                  new Metadata());
              return new ServerCall.Listener<>() {};
            }
            return next.startCall(call, headers);
          }
        };
    server =
        NettyServerBuilder.forAddress(new DomainSocketAddress(socket.toString()))
            .channelType(EpollServerDomainSocketChannel.class)
            .bossEventLoopGroup(group)
            .workerEventLoopGroup(group)
            .addService(ServerInterceptors.intercept(service, securityHeader))
            .build()
            .start();
  }

  private void fetch(StreamObserver<byte[]> responses) {
    Status failure = failures.isEmpty() ? failAll : failures.remove(0);
    if (failure != null) {
      responses.onError(failure.asRuntimeException());
      return;
    }
    // Under the lock, so that no response published, and no end of streams, falls between the
    // replay of the newest response and the subscription.
    synchronized (lock) {
      byte[] message = latest;
      if (message != null) {
        responses.onNext(message);
      }
      streams.add(responses);
    }
  }

  /** One SVID of a response. */
  public record Svid(String id, TestPki.Issued issued, List<X509Certificate> bundle, String hint) {

    /** An SVID whose ID is its certificate's, with its trust domain's bundle. */
    public static Svid of(TestPki.Issued issued, TestPki.Authority root) {
      return of(issued, root, "");
    }

    public static Svid of(TestPki.Issued issued, TestPki.Authority root, String hint) {
      String id = "";
      try {
        for (List<?> name : issued.leaf().getSubjectAlternativeNames()) {
          if ((Integer) name.get(0) == 6) {
            id = (String) name.get(1);
          }
        }
      } catch (java.security.cert.CertificateParsingException e) {
        throw new IllegalStateException(e);
      }
      return new Svid(id, issued, List.of(root.certificate()), hint);
    }
  }

  /** An {@code X509SVIDResponse}. */
  public static final class Response {
    private final List<Svid> svids = new ArrayList<>();
    private final Map<String, List<X509Certificate>> federated = new LinkedHashMap<>();
    private final List<byte[]> unknownFields = new ArrayList<>();

    public Response svid(Svid svid) {
      svids.add(svid);
      return this;
    }

    /** A federated bundle keyed as the agent keys it: {@code spiffe://<trust domain>}. */
    public Response federated(String key, List<X509Certificate> bundle) {
      federated.put(key, bundle);
      return this;
    }

    /** A field this client does not know, number 15, which it must skip. */
    public Response withUnknownField() {
      unknownFields.add(new byte[] {1, 2, 3});
      return this;
    }

    /** The protobuf encoding of the response. */
    public byte[] encode() {
      try {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        for (Svid svid : svids) {
          ByteArrayOutputStream inner = new ByteArrayOutputStream();
          CodedOutputStream message = CodedOutputStream.newInstance(inner);
          message.writeString(1, svid.id());
          message.writeByteArray(2, der(svid.issued().chain()));
          message.writeByteArray(3, svid.issued().key().getEncoded());
          message.writeByteArray(4, der(svid.bundle()));
          if (!svid.hint().isEmpty()) {
            message.writeString(5, svid.hint());
          }
          message.writeUInt64(14, 99);
          message.flush();
          out.writeByteArray(1, inner.toByteArray());
        }
        for (Map.Entry<String, List<X509Certificate>> entry : federated.entrySet()) {
          ByteArrayOutputStream inner = new ByteArrayOutputStream();
          CodedOutputStream message = CodedOutputStream.newInstance(inner);
          message.writeString(1, entry.getKey());
          message.writeByteArray(2, der(entry.getValue()));
          message.flush();
          out.writeByteArray(3, inner.toByteArray());
        }
        for (byte[] unknown : unknownFields) {
          out.writeByteArray(15, unknown);
          out.writeFixed32(16, 7);
          out.writeFixed64(17, 7L);
        }
        out.flush();
        return bytes.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private static byte[] der(List<X509Certificate> certificates) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (X509Certificate certificate : certificates) {
        try {
          out.writeBytes(certificate.getEncoded());
        } catch (CertificateEncodingException e) {
          throw new IllegalStateException(e);
        }
      }
      return out.toByteArray();
    }
  }
}
