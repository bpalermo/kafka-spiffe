package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.grpc.Status;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The source against the fake Workload API: first SVID, rotations, refusals, failures. */
class X509SourceTest {

  private static final TestPki.Authority OWN = TestPki.root("example.org");
  private static final TestPki.Authority FEDERATED = TestPki.root("k8s.example.org");
  private static final Clock CLOCK = Clock.fixed(TestPki.NOW, ZoneOffset.UTC);
  private static final Duration WAIT = Duration.ofSeconds(10);

  private Path directory;
  private FakeWorkloadApi api;
  private X509Source source;

  @BeforeEach
  void start() throws IOException {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    directory = Files.createTempDirectory(Path.of("/tmp"), "wl");
    api = FakeWorkloadApi.start(directory.resolve("agent.sock"));
  }

  @AfterEach
  void stop() throws Exception {
    if (source != null) {
      source.close();
    }
    if (api != null) {
      api.close();
    }
    if (directory != null) {
      Files.deleteIfExists(directory);
    }
  }

  private X509Source.Settings settings() {
    return new X509Source.Settings(
        api.socket(), "", null, Set.of(), Duration.ofMillis(20), Duration.ofMillis(200));
  }

  private static FakeWorkloadApi.Response response(TestPki.Issued issued) {
    return new FakeWorkloadApi.Response()
        .svid(FakeWorkloadApi.Svid.of(issued, OWN))
        .federated("spiffe://k8s.example.org", List.of(FEDERATED.certificate()));
  }

  private static void eventually(BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      Thread.sleep(10);
    }
  }

  @Test
  void deliversTheFirstSvidAndItsBundles() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    X509Context snapshot = source.awaitFirst(WAIT);
    assertEquals(SpiffeId.parse("spiffe://example.org/broker"), snapshot.svid().id());
    assertEquals(
        Set.of(TrustDomain.parse("example.org"), TrustDomain.parse("k8s.example.org")),
        snapshot.bundles().trustDomains());
    assertSame(snapshot, source.current());
    assertTrue(source.connected());
  }

  @Test
  void followsRotationsAndTellsListeners() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    X509Context first = source.awaitFirst(WAIT);
    List<X509Context> seen = new CopyOnWriteArrayList<>();
    source.addListener(seen::add);
    api.publish(response(OWN.svid("/broker")));
    eventually(() -> seen.size() == 1, "the rotation");
    assertNotSame(first, source.current());
    assertSame(seen.get(0), source.current());
    assertEquals(2, source.updates());
  }

  @Test
  void keepsTheCurrentSnapshotWhenAnUpdateIsRefused() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    X509Context first = source.awaitFirst(WAIT);

    TestPki.Issued one = OWN.svid("/broker");
    TestPki.Issued other = OWN.svid("/broker");
    api.publish(
        new FakeWorkloadApi.Response()
            .svid(
                new FakeWorkloadApi.Svid(
                    "spiffe://example.org/broker",
                    new TestPki.Issued(one.chain(), other.key()),
                    List.of(OWN.certificate()),
                    "")));
    eventually(() -> source.rejectedUpdates() == 1, "the refusal of a key that is not the leaf's");
    api.publish(new byte[] {0x0a, 0x05, 0x01});
    eventually(() -> source.rejectedUpdates() == 2, "the refusal of a malformed response");
    TestPki.Issued expired =
        OWN.leaf()
            .uris("spiffe://example.org/broker")
            .validity(
                TestPki.NOW.minus(Duration.ofHours(2)), TestPki.NOW.minus(Duration.ofHours(1)))
            .issue();
    api.publish(response(expired));
    eventually(() -> source.rejectedUpdates() == 3, "the refusal of an expired SVID");
    assertSame(first, source.current());
    assertEquals(1, source.updates());
  }

  @Test
  void picksTheSvidByHintOrId() throws Exception {
    FakeWorkloadApi.Response two =
        new FakeWorkloadApi.Response()
            .svid(FakeWorkloadApi.Svid.of(OWN.svid("/first"), OWN, "a"))
            .svid(FakeWorkloadApi.Svid.of(OWN.svid("/second"), OWN, "b"));
    api.publish(two);
    source =
        X509Source.start(
            new X509Source.Settings(
                api.socket(), "b", null, Set.of(), Duration.ofMillis(20), Duration.ofMillis(200)),
            CLOCK);
    assertEquals("spiffe://example.org/second", source.awaitFirst(WAIT).svid().id().toString());
    source.close();

    source =
        X509Source.start(
            new X509Source.Settings(
                api.socket(),
                "",
                SpiffeId.parse("spiffe://example.org/second"),
                Set.of(),
                Duration.ofMillis(20),
                Duration.ofMillis(200)),
            CLOCK);
    assertEquals("spiffe://example.org/second", source.awaitFirst(WAIT).svid().id().toString());
    source.close();

    source = X509Source.start(settings(), CLOCK);
    assertEquals("spiffe://example.org/first", source.awaitFirst(WAIT).svid().id().toString());
  }

  @Test
  void keepsOnlyTheBundlesOfTheTrustDomainsNamed() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source =
        X509Source.start(
            new X509Source.Settings(
                api.socket(),
                "",
                null,
                Set.of(TrustDomain.parse("example.org")),
                Duration.ofMillis(20),
                Duration.ofMillis(200)),
            CLOCK);
    assertEquals(
        Set.of(TrustDomain.parse("example.org")), source.awaitFirst(WAIT).bundles().trustDomains());
  }

  @Test
  void retriesPermissionDeniedUntilTheWorkloadIsRegistered() throws Exception {
    api.failNext(Status.PERMISSION_DENIED.withDescription("no identity issued"));
    api.failNext(Status.PERMISSION_DENIED.withDescription("no identity issued"));
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    source.awaitFirst(WAIT);
    assertEquals(3, api.calls());
    assertEquals(2, source.streamErrors());
  }

  @Test
  void reconnectsWhenTheStreamEndsOrTheAgentRestarts() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    X509Context first = source.awaitFirst(WAIT);

    api.endStreams(Status.UNAVAILABLE);
    eventually(() -> source.updates() == 2, "the snapshot after the stream ended");

    api.stop();
    eventually(() -> !source.connected(), "the disconnection");
    assertEquals(first.svid().id(), source.current().svid().id());
    api.publish(response(OWN.svid("/broker")));
    api.restart();
    eventually(() -> source.updates() == 3, "the snapshot after the agent came back");
    assertTrue(source.streamErrors() >= 2);
  }

  @Test
  void stopsOnInvalidArgument() throws Exception {
    api.failAll(Status.INVALID_ARGUMENT.withDescription("bad request"));
    source = X509Source.start(settings(), CLOCK);
    TimeoutException e = assertThrows(TimeoutException.class, () -> source.awaitFirst(WAIT));
    assertTrue(e.getMessage().contains("INVALID_ARGUMENT"), e.getMessage());
    assertTrue(source.stopped());
    assertEquals(1, api.calls());
  }

  @Test
  void awaitingSaysWhyThereIsNoSvid() throws Exception {
    api.failAll(Status.PERMISSION_DENIED.withDescription("no identity issued"));
    source = X509Source.start(settings(), CLOCK);
    TimeoutException e =
        assertThrows(TimeoutException.class, () -> source.awaitFirst(Duration.ofMillis(300)));
    assertTrue(e.getMessage().contains("PERMISSION_DENIED"), e.getMessage());
    IllegalStateException none = assertThrows(IllegalStateException.class, source::current);
    assertTrue(none.getMessage().contains("PERMISSION_DENIED"), none.getMessage());
    assertThrows(TimeoutException.class, () -> source.awaitFirst(Duration.ZERO));
  }

  @Test
  void anAbsentAgentIsRetried() throws Exception {
    api.stop();
    source = X509Source.start(settings(), CLOCK);
    eventually(() -> source.streamErrors() >= 2, "two failed connections");
    api.publish(response(OWN.svid("/broker")));
    api.restart();
    source.awaitFirst(WAIT);
  }

  @Test
  void closingStopsTheSource() throws Exception {
    api.publish(response(OWN.svid("/broker")));
    source = X509Source.start(settings(), CLOCK);
    source.awaitFirst(WAIT);
    source.close();
    eventually(() -> !source.connected(), "the source to stop");
    int calls = api.calls();
    Thread.sleep(300);
    assertEquals(calls, api.calls());
    assertFalse(source.stopped());
  }

  @Test
  void jitterStaysBetweenHalfAndAllOfTheDelay() {
    for (int i = 0; i < 1000; i++) {
      long millis = X509Source.jittered(Duration.ofMillis(1000)).toMillis();
      assertTrue(millis >= 500 && millis <= 1000, String.valueOf(millis));
    }
  }
}
