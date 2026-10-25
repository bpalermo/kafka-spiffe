package io.github.bpalermo.kafka.spiffe.kafka;

import java.util.function.LongSupplier;

/**
 * At most {@code permits} log lines a window, for messages a peer can cause at will; the next line
 * after a quiet spell says how many were dropped.
 */
final class LogLimiter {

  private final int permits;
  private final long windowNanos;
  private final LongSupplier nanoTime;
  private long windowStart;
  private int used;
  private long suppressed;

  LogLimiter(int permits, long windowMillis) {
    this(permits, windowMillis, System::nanoTime);
  }

  LogLimiter(int permits, long windowMillis, LongSupplier nanoTime) {
    this.permits = permits;
    this.windowNanos = windowMillis * 1_000_000L;
    this.nanoTime = nanoTime;
    this.windowStart = nanoTime.getAsLong();
  }

  /**
   * Takes a permit.
   *
   * @return -1 if the line must not be logged, else how many lines were dropped since the last one
   *     logged
   */
  synchronized long acquire() {
    long now = nanoTime.getAsLong();
    if (now - windowStart >= windowNanos) {
      windowStart = now;
      used = 0;
    }
    if (used >= permits) {
      suppressed++;
      return -1;
    }
    used++;
    long dropped = suppressed;
    suppressed = 0;
    return dropped;
  }
}
