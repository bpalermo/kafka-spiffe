package io.github.bpalermo.kafka.spiffe.x509;

/** An SVID or a snapshot that cannot be used, and why. */
public final class InvalidSvidException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidSvidException(String message) {
    super(message);
  }

  public InvalidSvidException(String message, Throwable cause) {
    super(message, cause);
  }
}
