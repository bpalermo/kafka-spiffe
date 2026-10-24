package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import io.netty.channel.epoll.Epoll;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class X509SourceRegistryTest {

  private static final TestPki.Authority OWN = TestPki.root("example.org");

  @Test
  void sharesOneSourcePerSettingsUntilTheLastHandleIsClosed() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    Path directory = Files.createTempDirectory(Path.of("/tmp"), "reg");
    try (FakeWorkloadApi api = FakeWorkloadApi.start(directory.resolve("agent.sock"))) {
      api.publish(
          new FakeWorkloadApi.Response().svid(FakeWorkloadApi.Svid.of(OWN.svid("/a"), OWN)));
      X509SourceRegistry registry =
          new X509SourceRegistry(Clock.fixed(TestPki.NOW, ZoneOffset.UTC));
      X509Source.Settings settings = X509Source.Settings.of(api.socket());

      X509MaterialSource one = registry.acquire(settings);
      X509MaterialSource two = registry.acquire(settings);
      assertEquals(1, registry.size());
      assertSame(one.awaitFirst(Duration.ofSeconds(10)), two.awaitFirst(Duration.ofSeconds(10)));
      assertEquals(1, api.calls());

      one.close();
      one.close();
      assertEquals(1, registry.size());
      two.current();
      two.close();
      assertEquals(0, registry.size());

      X509MaterialSource again = registry.acquire(settings);
      again.awaitFirst(Duration.ofSeconds(10));
      assertEquals(2, api.calls());
      again.close();
    } finally {
      Files.deleteIfExists(directory);
    }
  }

  @Test
  void aRunningSourceHasItsMBean() throws Exception {
    Assumptions.assumeTrue(Epoll.isAvailable(), "the fake Workload API needs Netty's epoll");
    Path directory = Files.createTempDirectory(Path.of("/tmp"), "mb");
    try (FakeWorkloadApi api = FakeWorkloadApi.start(directory.resolve("agent.sock"))) {
      api.publish(
          new FakeWorkloadApi.Response()
              .svid(FakeWorkloadApi.Svid.of(OWN.svid("/a"), OWN))
              .federated(
                  "spiffe://k8s.example.org",
                  java.util.List.of(TestPki.root("k8s.example.org").certificate())));
      X509SourceRegistry registry =
          new X509SourceRegistry(Clock.fixed(TestPki.NOW, ZoneOffset.UTC), "kafka.spiffe.test");
      javax.management.MBeanServer server =
          java.lang.management.ManagementFactory.getPlatformMBeanServer();
      javax.management.ObjectName name =
          new javax.management.ObjectName(
              "kafka.spiffe.test:type=x509-source,socket="
                  + javax.management.ObjectName.quote(api.socket().toString()));
      X509MaterialSource source = registry.acquire(X509Source.Settings.of(api.socket()));
      source.awaitFirst(Duration.ofSeconds(10));
      assertEquals(true, server.getAttribute(name, "connected"));
      assertEquals(1L, server.getAttribute(name, "updates-total"));
      assertEquals(0L, server.getAttribute(name, "updates-rejected-total"));
      assertEquals(2L, server.getAttribute(name, "bundle-count"));
      assertEquals(
          "example.org,k8s.example.org", server.getAttribute(name, "bundle-trust-domains"));
      assertEquals(30 * 60 * 1000L, server.getAttribute(name, "svid-ttl-remaining-ms"));
      assertEquals(
          source.current().expiresAt().toEpochMilli(),
          server.getAttribute(name, "svid-expiry-timestamp-ms"));
      assertEquals(
          TestPki.NOW.toEpochMilli(), server.getAttribute(name, "last-update-timestamp-ms"));
      source.close();
      org.junit.jupiter.api.Assertions.assertFalse(server.isRegistered(name));
    } finally {
      Files.deleteIfExists(directory);
    }
  }
}
