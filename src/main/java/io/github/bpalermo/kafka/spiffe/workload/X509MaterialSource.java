package io.github.bpalermo.kafka.spiffe.workload;

import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Where snapshots come from: the Workload API ({@link X509Source}), or anything else that can
 * produce validated {@link X509Context}s, such as files.
 *
 * <p>Public API (design, section 3): an application that needs, outside Kafka, the same SVIDs and
 * bundles its Kafka clients use gets a handle on the shared source from {@link
 * X509SourceRegistry#acquire}, or from {@code SpiffeConfig.acquireSource()} for the source of a set
 * of {@code spiffe.*} keys.
 */
public interface X509MaterialSource extends AutoCloseable {

  /**
   * What a source has done so far: the numbers its MBean shows (design, section 7), readable
   * without JMX.
   *
   * @param connected whether its stream to the Workload API is open; true for a source without one
   * @param updates how many snapshots replaced the current one, the first included
   * @param rejectedUpdates how many updates were refused, the current snapshot kept
   * @param streamErrors how many times its stream ended or could not be opened
   * @param lastUpdate when the snapshot was last replaced, or null if it never was
   */
  record Stats(
      boolean connected,
      long updates,
      long rejectedUpdates,
      long streamErrors,
      Instant lastUpdate) {

    /** A source that reports nothing: connected, no counts. */
    public static final Stats NONE = new Stats(true, 0, 0, 0, null);
  }

  /**
   * The newest valid snapshot.
   *
   * @throws IllegalStateException if there has been none yet
   */
  X509Context current();

  /**
   * Waits for the first valid snapshot: for ever if {@code timeout} is negative, not at all if it
   * is zero.
   *
   * @throws TimeoutException if none came in time, saying why if the source knows
   */
  X509Context awaitFirst(Duration timeout) throws TimeoutException, InterruptedException;

  /** Calls {@code listener} with every new snapshot, on the source's thread, until removed. */
  void addListener(Consumer<X509Context> listener);

  void removeListener(Consumer<X509Context> listener);

  /** What the source has done so far. {@link Stats#NONE} unless the source counts. */
  default Stats stats() {
    return Stats.NONE;
  }

  /** Stops the source; snapshots already handed out stay usable. */
  @Override
  void close();
}
