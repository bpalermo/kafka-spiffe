package io.github.bpalermo.kafka.spiffe.workload.h2;

/**
 * How a gRPC call ended: one of gRPC's status codes and a message.
 *
 * @param code the status
 * @param message what the server, or this client, said about it; empty if nothing
 */
public record GrpcStatus(Code code, String message) {

  /** gRPC's status codes, with their numbers. */
  public enum Code {
    OK,
    CANCELLED,
    UNKNOWN,
    INVALID_ARGUMENT,
    DEADLINE_EXCEEDED,
    NOT_FOUND,
    ALREADY_EXISTS,
    PERMISSION_DENIED,
    RESOURCE_EXHAUSTED,
    FAILED_PRECONDITION,
    ABORTED,
    OUT_OF_RANGE,
    UNIMPLEMENTED,
    INTERNAL,
    UNAVAILABLE,
    DATA_LOSS,
    UNAUTHENTICATED;

    /** The code with {@code number}, or UNKNOWN for a number gRPC does not define. */
    public static Code of(int number) {
      Code[] codes = values();
      return number >= 0 && number < codes.length ? codes[number] : UNKNOWN;
    }
  }

  public GrpcStatus {
    message = message == null ? "" : message;
  }

  static GrpcStatus of(Code code, String message) {
    return new GrpcStatus(code, message);
  }

  /** The status gRPC gives an HTTP response without {@code grpc-status}. */
  static GrpcStatus ofHttp(int status) {
    Code code =
        switch (status) {
          case 400 -> Code.INTERNAL;
          case 401 -> Code.UNAUTHENTICATED;
          case 403 -> Code.PERMISSION_DENIED;
          case 404 -> Code.UNIMPLEMENTED;
          case 429, 502, 503, 504 -> Code.UNAVAILABLE;
          default -> Code.UNKNOWN;
        };
    return new GrpcStatus(code, "HTTP status " + status + " without grpc-status");
  }

  /** The status gRPC gives a stream the server reset with an HTTP/2 error code. */
  static GrpcStatus ofReset(long http2ErrorCode) {
    Code code =
        switch ((int) http2ErrorCode) {
          case Http2.REFUSED_STREAM -> Code.UNAVAILABLE;
          case Http2.CANCEL -> Code.CANCELLED;
          case Http2.ENHANCE_YOUR_CALM -> Code.RESOURCE_EXHAUSTED;
          case Http2.INADEQUATE_SECURITY -> Code.PERMISSION_DENIED;
          default -> Code.INTERNAL;
        };
    return new GrpcStatus(code, "stream reset by the server with HTTP/2 error " + http2ErrorCode);
  }

  public boolean isOk() {
    return code == Code.OK;
  }

  @Override
  public String toString() {
    return message.isEmpty() ? code.name() : code + ": " + message;
  }
}
