package io.github.bpalermo.kafka.spiffe.workload;

import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Where snapshots come from: the Workload API ({@link X509Source}), or anything else that can
 * produce validated {@link X509Context}s, such as files.
 */
public interface X509MaterialSource extends AutoCloseable {

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

  /** Stops the source; snapshots already handed out stay usable. */
  @Override
  void close();
}
