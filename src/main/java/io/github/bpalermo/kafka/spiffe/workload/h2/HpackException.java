package io.github.bpalermo.kafka.spiffe.workload.h2;

/** A header block that cannot be decoded: an HTTP/2 connection error of type COMPRESSION_ERROR. */
final class HpackException extends Exception {
  private static final long serialVersionUID = 1L;

  HpackException(String message) {
    super(message);
  }
}
