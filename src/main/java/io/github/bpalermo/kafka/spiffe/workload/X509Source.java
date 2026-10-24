package io.github.bpalermo.kafka.spiffe.workload;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.metrics.GaugeMBean;
import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcCall;
import io.github.bpalermo.kafka.spiffe.workload.h2.GrpcStatus;
import io.github.bpalermo.kafka.spiffe.x509.BundleSet;
import io.github.bpalermo.kafka.spiffe.x509.InvalidSvidException;
import io.github.bpalermo.kafka.spiffe.x509.SvidValidator;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.github.bpalermo.kafka.spiffe.x509.X509Svid;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.JMException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * X.509 snapshots from the Workload API: one {@code FetchX509SVID} stream kept open on a daemon
 * thread of its own, reconnected with backoff and jitter when it ends.
 *
 * <p>Each response is decoded, its SVID picked (by hint, by SPIFFE ID, or the first), its bundles
 * narrowed to the trust domains configured, and the result validated ({@link SvidValidator}) before
 * it replaces the current snapshot; a response that fails keeps the current one (design, section
 * 6). {@code PERMISSION_DENIED}, which an agent returns while it has no registration entry for the
 * workload, is retried with a warning; {@code INVALID_ARGUMENT} is not retried.
 */
public final class X509Source implements X509MaterialSource {

  /** The Workload API method that streams X.509 material. */
  static final String METHOD = "/SpiffeWorkloadAPI/FetchX509SVID";

  /** The header every Workload API call must carry. */
  static final Map<String, String> SECURITY_HEADER = Map.of("workload.spiffe.io", "true");

  private static final Logger LOG = LoggerFactory.getLogger(X509Source.class);

  /**
   * How a source is set up. Settings of equal {@link #identity()} share one source ({@link
   * X509SourceRegistry}), whatever their backoff.
   *
   * @param socket the agent's Unix domain socket
   * @param svidHint pick the SVID with this hint; empty for none
   * @param svidId pick the SVID with this SPIFFE ID; null for none
   * @param bundleTrustDomains keep only these trust domains' bundles; empty for all
   * @param backoff the first reconnect delay
   * @param maxBackoff the longest reconnect delay
   */
  public record Settings(
      Path socket,
      String svidHint,
      SpiffeId svidId,
      Set<TrustDomain> bundleTrustDomains,
      Duration backoff,
      Duration maxBackoff) {

    public Settings {
      if (socket == null) {
        throw new IllegalArgumentException("the Workload API socket is required");
      }
      svidHint = svidHint == null ? "" : svidHint;
      bundleTrustDomains = Set.copyOf(bundleTrustDomains);
      if (backoff.isNegative() || backoff.isZero() || maxBackoff.compareTo(backoff) < 0) {
        throw new IllegalArgumentException(
            "the backoff must be positive and no longer than the maximum");
      }
    }

    /** What makes two sources different: everything but the backoff. */
    public Identity identity() {
      return new Identity(socket, svidHint, svidId, bundleTrustDomains);
    }

    /** A socket's settings with the defaults: the first SVID, every bundle, 500 ms to 30 s. */
    public static Settings of(Path socket) {
      return new Settings(
          socket, "", null, Set.of(), Duration.ofMillis(500), Duration.ofSeconds(30));
    }
  }

  /**
   * Which snapshots a source produces: from which socket, which SVID, which bundles.
   *
   * @param socket the agent's socket
   * @param svidHint the hint the SVID is picked by, or empty
   * @param svidId the ID the SVID is picked by, or null
   * @param bundleTrustDomains the trust domains whose bundles are kept, or empty for all
   */
  public record Identity(
      Path socket, String svidHint, SpiffeId svidId, Set<TrustDomain> bundleTrustDomains) {}

  private final Settings settings;
  private final Clock clock;
  private final Thread thread;
  private final CountDownLatch first = new CountDownLatch(1);
  private final List<Consumer<X509Context>> listeners = new CopyOnWriteArrayList<>();
  private final AtomicLong updates = new AtomicLong();
  private final AtomicLong rejected = new AtomicLong();
  private final AtomicLong streamErrors = new AtomicLong();
  private volatile X509Context current;
  private volatile Instant lastUpdate;
  private volatile String lastError = "no response from the Workload API yet";
  private volatile boolean connected;
  private volatile boolean stopped;
  private volatile boolean closed;
  private volatile GrpcCall call;
  private volatile boolean updatedSinceConnect;
  private volatile X509Context warnedLate;
  private volatile X509Context warnedExpired;

  private X509Source(Settings settings, Clock clock) {
    this.settings = settings;
    this.clock = clock;
    this.thread = new Thread(this::run, "kafka-spiffe-workload-api " + settings.socket());
    this.thread.setDaemon(true);
  }

  /** Starts a source; it connects in the background. */
  public static X509Source start(Settings settings, Clock clock) {
    X509Source source = new X509Source(settings, clock);
    source.thread.start();
    return source;
  }

  public Settings settings() {
    return settings;
  }

  @Override
  public X509Context current() {
    X509Context snapshot = current;
    if (snapshot == null) {
      throw new IllegalStateException(
          "no X.509 SVID from the Workload API at " + settings.socket() + " yet: " + lastError);
    }
    checkExpiry(snapshot);
    return snapshot;
  }

  @Override
  public X509Context awaitFirst(Duration timeout) throws TimeoutException, InterruptedException {
    boolean arrived =
        timeout.isNegative()
            ? waitForever()
            : first.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    X509Context snapshot = current;
    if (!arrived || snapshot == null) {
      throw new TimeoutException(
          "no X.509 SVID from the Workload API at "
              + settings.socket()
              + " within "
              + timeout
              + ": "
              + lastError);
    }
    return snapshot;
  }

  private boolean waitForever() throws InterruptedException {
    first.await();
    return true;
  }

  @Override
  public void addListener(Consumer<X509Context> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<X509Context> listener) {
    listeners.remove(listener);
  }

  @Override
  public void close() {
    closed = true;
    GrpcCall running = call;
    if (running != null) {
      running.close();
    }
    thread.interrupt();
    first.countDown();
  }

  /** Whether the stream is open now. */
  public boolean connected() {
    return connected;
  }

  /** How many responses replaced the snapshot. */
  public long updates() {
    return updates.get();
  }

  /** How many responses were refused, the snapshot kept. */
  public long rejectedUpdates() {
    return rejected.get();
  }

  /** How many times the stream ended or could not be opened. */
  public long streamErrors() {
    return streamErrors.get();
  }

  /** When the snapshot was last replaced, or null. */
  public Instant lastUpdate() {
    return lastUpdate;
  }

  /** Whether the source gave up, after INVALID_ARGUMENT. */
  public boolean stopped() {
    return stopped;
  }

  /**
   * Registers the source's MBean, {@code <domain>:type=x509-source,socket=<path>} (design, section
   * 7), and returns what unregisters it. A source that picks its SVID or narrows its bundles adds
   * {@code svid-hint}, {@code svid-id} or {@code bundles} to the name, so that two sources on one
   * socket have an MBean each.
   *
   * @throws JMException if the platform MBean server refuses it
   */
  public GaugeMBean.Registration registerMBean(String domain) throws JMException {
    ObjectName name = objectName(domain, settings.identity());
    return GaugeMBean.builder("The Workload API source of " + settings.socket())
        .booleanAttribute("connected", "Whether the stream is open", () -> connected)
        .longAttribute("updates-total", "Responses that replaced the snapshot", updates::get)
        .longAttribute(
            "updates-rejected-total", "Responses refused, the snapshot kept", rejected::get)
        .longAttribute(
            "stream-errors-total",
            "Times the stream ended or could not be opened",
            streamErrors::get)
        .longAttribute(
            "last-update-timestamp-ms",
            "When the snapshot was last replaced, or -1",
            () -> lastUpdate == null ? -1L : lastUpdate.toEpochMilli())
        .longAttribute(
            "svid-expiry-timestamp-ms",
            "When the SVID expires, or -1",
            () -> current == null ? -1L : current.expiresAt().toEpochMilli())
        .longAttribute(
            "svid-ttl-remaining-ms",
            "How long until the SVID expires, negative once it has, or -1 without one",
            () ->
                current == null
                    ? -1L
                    : Duration.between(clock.instant(), current.expiresAt()).toMillis())
        .longAttribute(
            "bundle-count",
            "Trust domains with a bundle",
            () -> current == null ? 0L : current.bundles().trustDomains().size())
        .stringAttribute(
            "bundle-trust-domains",
            "The trust domains with a bundle, sorted",
            () ->
                current == null
                    ? ""
                    : current.bundles().trustDomains().stream()
                        .map(TrustDomain::name)
                        .sorted()
                        .collect(Collectors.joining(",")))
        .register(name);
  }

  private void run() {
    Duration delay = settings.backoff();
    while (!closed) {
      updatedSinceConnect = false;
      GrpcStatus status;
      try {
        GrpcCall opened = GrpcCall.connect(settings.socket());
        call = opened;
        if (closed) {
          opened.close();
          break;
        }
        connected = true;
        status = opened.run(METHOD, SECURITY_HEADER, new byte[0], this::onResponse);
      } catch (IOException e) {
        status = new GrpcStatus(GrpcStatus.Code.UNAVAILABLE, "cannot connect: " + e.getMessage());
      } finally {
        connected = false;
        call = null;
      }
      if (closed) {
        break;
      }
      streamErrors.incrementAndGet();
      lastError = "the stream ended with " + status;
      if (status.code() == GrpcStatus.Code.INVALID_ARGUMENT) {
        LOG.error(
            "The Workload API at {} refused the request ({}); not retrying",
            settings.socket(),
            status);
        stopped = true;
        first.countDown();
        return;
      }
      if (status.code() == GrpcStatus.Code.PERMISSION_DENIED) {
        LOG.warn(
            "The Workload API at {} has no SVID for this workload yet ({}): is it registered?",
            settings.socket(),
            status);
      } else {
        LOG.warn("The Workload API stream at {} ended: {}", settings.socket(), status);
      }
      X509Context snapshot = current;
      if (snapshot != null) {
        checkExpiry(snapshot);
      }
      if (updatedSinceConnect) {
        delay = settings.backoff();
      }
      try {
        Thread.sleep(jittered(delay).toMillis());
      } catch (InterruptedException e) {
        if (closed) {
          break;
        }
        Thread.currentThread().interrupt();
        break;
      }
      Duration doubled = delay.multipliedBy(2);
      delay = doubled.compareTo(settings.maxBackoff()) > 0 ? settings.maxBackoff() : doubled;
    }
  }

  /** The MBean name of a source of {@code identity}. */
  static ObjectName objectName(String domain, Identity identity) throws JMException {
    StringBuilder name =
        new StringBuilder(domain)
            .append(":type=x509-source,socket=")
            .append(ObjectName.quote(identity.socket().toString()));
    if (!identity.svidHint().isEmpty()) {
      name.append(",svid-hint=").append(ObjectName.quote(identity.svidHint()));
    }
    if (identity.svidId() != null) {
      name.append(",svid-id=").append(ObjectName.quote(identity.svidId().toString()));
    }
    if (!identity.bundleTrustDomains().isEmpty()) {
      name.append(",bundles=")
          .append(
              ObjectName.quote(
                  identity.bundleTrustDomains().stream()
                      .map(TrustDomain::name)
                      .sorted()
                      .collect(Collectors.joining(","))));
    }
    return new ObjectName(name.toString());
  }

  /** A delay between half and all of {@code delay}. */
  static Duration jittered(Duration delay) {
    long millis = delay.toMillis();
    return Duration.ofMillis(millis / 2 + ThreadLocalRandom.current().nextLong(millis / 2 + 1));
  }

  private void onResponse(byte[] message) {
    X509Context snapshot;
    try {
      snapshot = snapshot(X509SvidResponse.parse(message));
      SvidValidator.validate(snapshot, clock.instant());
    } catch (X509SvidResponse.InvalidResponseException | InvalidSvidException e) {
      rejected.incrementAndGet();
      lastError = "a response was refused: " + e.getMessage();
      LOG.warn(
          "Refused an update from the Workload API at {}, keeping the current SVID: {}",
          settings.socket(),
          e.getMessage());
      return;
    }
    current = snapshot;
    lastUpdate = clock.instant();
    updates.incrementAndGet();
    updatedSinceConnect = true;
    first.countDown();
    LOG.info(
        "New X.509 SVID {} from the Workload API, expires {}, bundles of {}",
        snapshot.svid().id(),
        snapshot.expiresAt(),
        snapshot.bundles().trustDomains());
    for (Consumer<X509Context> listener : listeners) {
      try {
        listener.accept(snapshot);
      } catch (RuntimeException e) {
        LOG.error("A listener of the X.509 source failed", e);
      }
    }
  }

  private X509Context snapshot(X509SvidResponse response)
      throws X509SvidResponse.InvalidResponseException {
    X509Svid chosen = null;
    for (X509Svid svid : response.svids()) {
      boolean hintMatches =
          settings.svidHint().isEmpty() || settings.svidHint().equals(svid.hint());
      boolean idMatches = settings.svidId() == null || settings.svidId().equals(svid.id());
      if (hintMatches && idMatches) {
        chosen = svid;
        break;
      }
    }
    if (chosen == null) {
      throw new X509SvidResponse.InvalidResponseException(
          "no SVID matches"
              + (settings.svidHint().isEmpty() ? "" : " hint " + settings.svidHint())
              + (settings.svidId() == null ? "" : " ID " + settings.svidId())
              + " among "
              + response.svids().size());
    }
    BundleSet bundles = BundleSet.of(response.bundles().values());
    if (!settings.bundleTrustDomains().isEmpty()) {
      bundles = bundles.retainOnly(settings.bundleTrustDomains());
    }
    return new X509Context(chosen, bundles);
  }

  /** Logs, once per snapshot, a rotation that is late and an SVID that has expired. */
  private void checkExpiry(X509Context snapshot) {
    Instant now = clock.instant();
    Instant notBefore = snapshot.svid().leaf().getNotBefore().toInstant();
    Instant notAfter = snapshot.expiresAt();
    if (!now.isBefore(notAfter)) {
      if (warnedExpired != snapshot) {
        warnedExpired = snapshot;
        LOG.error(
            "The X.509 SVID {} expired at {} and the Workload API at {} has not replaced it: {}",
            snapshot.svid().id(),
            notAfter,
            settings.socket(),
            lastError);
      }
      return;
    }
    Duration lifetime = Duration.between(notBefore, notAfter);
    if (now.isAfter(notBefore.plus(lifetime.multipliedBy(2).dividedBy(3)))
        && warnedLate != snapshot) {
      warnedLate = snapshot;
      LOG.warn(
          "The X.509 SVID {} is two thirds through its lifetime (expires {}) and not yet rotated:"
              + " {}",
          snapshot.svid().id(),
          notAfter,
          lastError);
    }
  }
}
