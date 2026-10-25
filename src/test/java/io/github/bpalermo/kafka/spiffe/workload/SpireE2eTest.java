package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.Spire;
import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcCall;
import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcStatus;
import io.github.bpalermo.kafka.spiffe.x509.SvidValidator;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The client against a real SPIRE agent (tagged {@code e2e}): the first SVID, a rotation, an agent
 * restart, and one response recorded as a test fixture.
 */
class SpireE2eTest {

  private static final Duration WAIT = Duration.ofSeconds(120);

  private static Spire spire;

  @BeforeAll
  static void start() throws Exception {
    spire = Spire.start("example.org", Duration.ofSeconds(60));
  }

  @AfterAll
  static void stop() {
    if (spire != null) {
      spire.close();
    }
  }

  private static void eventually(BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(200);
    }
  }

  private static X509Source.Settings settings() {
    return new X509Source.Settings(
        spire.socket(), "", null, Set.of(), Duration.ofMillis(200), Duration.ofSeconds(2));
  }

  @Test
  void fetchesFollowsRotationsAndSurvivesAnAgentRestart() throws Exception {
    try (X509Source source = X509Source.start(settings(), Clock.systemUTC())) {
      X509Context first = source.awaitFirst(WAIT);
      assertEquals(spire.workloadId(), first.svid().id().toString());
      assertTrue(first.bundles().trustDomains().contains(TrustDomain.parse("example.org")));
      SvidValidator.validate(first, Instant.now());
      Duration lifetime =
          Duration.between(first.svid().leaf().getNotBefore().toInstant(), first.expiresAt());
      assertTrue(lifetime.compareTo(Duration.ofMinutes(2)) <= 0, "SVID lifetime " + lifetime);

      long updates = source.updates();
      eventually(() -> source.updates() > updates, "a rotation");
      assertTrue(source.current().expiresAt().isAfter(first.expiresAt()));

      long beforeRestart = source.updates();
      spire.restartAgent();
      eventually(
          () -> source.updates() > beforeRestart && source.connected(),
          "the snapshot after the agent restarted");
      SvidValidator.validate(source.current(), Instant.now());
    }
  }

  /** Records one real response, for the fixture the unit tests read (see AGENTS.md). */
  @Test
  void recordsAResponse() throws Exception {
    AtomicReference<byte[]> recorded = new AtomicReference<>();
    long deadline = System.nanoTime() + WAIT.toNanos();
    ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    try {
      while (recorded.get() == null && System.nanoTime() < deadline) {
        GrpcCall call = GrpcCall.connect(spire.socket());
        // The call reads a blocking socket: closing it is what ends a stream that never answers.
        ScheduledFuture<?> expiry =
            timer.schedule(call::close, deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        GrpcStatus status =
            call.run(
                X509Source.METHOD,
                X509Source.SECURITY_HEADER,
                new byte[0],
                message -> {
                  recorded.set(message);
                  call.close();
                });
        expiry.cancel(false);
        if (recorded.get() == null) {
          assertTrue(status.code() != GrpcStatus.Code.INVALID_ARGUMENT, status.toString());
          Thread.sleep(500);
        }
      }
    } finally {
      timer.shutdownNow();
    }
    assertTrue(recorded.get() != null, "no response from the agent within " + WAIT);
    X509SvidResponse response = X509SvidResponse.parse(recorded.get());
    assertEquals(spire.workloadId(), response.svids().get(0).id().toString());
    String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
    if (outputs != null) {
      Files.write(Path.of(outputs, "spire-x509-svid-response.bin"), recorded.get());
    }
  }
}
