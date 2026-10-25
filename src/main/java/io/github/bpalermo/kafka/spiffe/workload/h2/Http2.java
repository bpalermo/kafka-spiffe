package io.github.bpalermo.kafka.spiffe.workload.h2;

import java.nio.charset.StandardCharsets;

/** HTTP/2's constants (RFC 9113): frame types, flags, settings and error codes. */
final class Http2 {

  /** What a client sends first, before its SETTINGS frame. */
  static final byte[] PREFACE =
      "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

  static final int FRAME_HEADER_LENGTH = 9;

  /** The default and minimum SETTINGS_MAX_FRAME_SIZE, which this client keeps. */
  static final int MAX_FRAME_SIZE = 16_384;

  /** The initial flow-control window of a connection and, by default, of a stream. */
  static final int DEFAULT_WINDOW = 65_535;

  static final int DATA = 0x0;
  static final int HEADERS = 0x1;
  static final int PRIORITY = 0x2;
  static final int RST_STREAM = 0x3;
  static final int SETTINGS = 0x4;
  static final int PUSH_PROMISE = 0x5;
  static final int PING = 0x6;
  static final int GOAWAY = 0x7;
  static final int WINDOW_UPDATE = 0x8;
  static final int CONTINUATION = 0x9;

  static final int FLAG_END_STREAM = 0x1;
  static final int FLAG_ACK = 0x1;
  static final int FLAG_END_HEADERS = 0x4;
  static final int FLAG_PADDED = 0x8;
  static final int FLAG_PRIORITY = 0x20;

  static final int SETTINGS_HEADER_TABLE_SIZE = 0x1;
  static final int SETTINGS_ENABLE_PUSH = 0x2;
  static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;
  static final int SETTINGS_MAX_FRAME_SIZE = 0x5;

  static final int NO_ERROR = 0x0;
  static final int PROTOCOL_ERROR = 0x1;
  static final int INTERNAL_ERROR = 0x2;
  static final int FLOW_CONTROL_ERROR = 0x3;
  static final int FRAME_SIZE_ERROR = 0x6;
  static final int REFUSED_STREAM = 0x7;
  static final int CANCEL = 0x8;
  static final int COMPRESSION_ERROR = 0x9;
  static final int ENHANCE_YOUR_CALM = 0xb;
  static final int INADEQUATE_SECURITY = 0xc;

  private Http2() {}

  /** A connection error this client detected: the code it sends in its GOAWAY. */
  static final class ConnectionError extends Exception {
    private static final long serialVersionUID = 1L;

    private final int code;

    ConnectionError(int code, String message) {
      super(message);
      this.code = code;
    }

    ConnectionError(int code, String message, Throwable cause) {
      super(message, cause);
      this.code = code;
    }

    int code() {
      return code;
    }
  }
}
