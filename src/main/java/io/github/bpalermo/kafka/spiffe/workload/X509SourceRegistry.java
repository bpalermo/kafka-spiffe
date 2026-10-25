package io.github.bpalermo.kafka.spiffe.workload;

import io.github.bpalermo.kafka.spiffe.metrics.GaugeMBean;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.management.JMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One {@link X509Source} per set of settings per process, shared and reference-counted: a broker
 * makes an engine factory per network thread per listener and per inter-broker client (phase 0),
 * and all of them read the same stream.
 */
public final class X509SourceRegistry {

  /** The JMX domain of the shared registry's MBeans. */
  public static final String JMX_DOMAIN = "kafka.spiffe";

  private static final Logger LOG = LoggerFactory.getLogger(X509SourceRegistry.class);

  private static final X509SourceRegistry SHARED =
      new X509SourceRegistry(Clock.systemUTC(), JMX_DOMAIN);

  private final Clock clock;
  private final String jmxDomain;
  private final Map<X509Source.Identity, Entry> sources = new HashMap<>();

  /** A registry of its own, for tests, without MBeans. */
  public X509SourceRegistry(Clock clock) {
    this(clock, null);
  }

  /**
   * A registry whose sources register their MBean in {@code jmxDomain} while they run, or none if
   * it is null.
   */
  public X509SourceRegistry(Clock clock, String jmxDomain) {
    this.clock = clock;
    this.jmxDomain = jmxDomain;
  }

  private static final Map<String, X509SourceRegistry> SHARED_BY_DOMAIN = new HashMap<>();

  /** The registry of the process. */
  public static X509SourceRegistry shared() {
    return SHARED;
  }

  /**
   * The registry of the process whose sources register their MBeans in {@code jmxDomain}, or none
   * if it is null. Factories configured alike share one.
   */
  public static X509SourceRegistry shared(String jmxDomain) {
    if (JMX_DOMAIN.equals(jmxDomain)) {
      return SHARED;
    }
    synchronized (SHARED_BY_DOMAIN) {
      return SHARED_BY_DOMAIN.computeIfAbsent(
          jmxDomain == null ? "" : jmxDomain,
          domain -> new X509SourceRegistry(Clock.systemUTC(), domain.isEmpty() ? null : domain));
    }
  }

  /**
   * A handle on the source of {@code settings}, started if it is not running. Closing the handle
   * releases it; the source stops when the last handle is closed. Settings of the same identity
   * share the source started first, and its backoff.
   */
  public synchronized X509MaterialSource acquire(X509Source.Settings settings) {
    X509Source.Identity identity = settings.identity();
    Entry entry = sources.get(identity);
    if (entry != null && !entry.source.settings().equals(settings)) {
      LOG.info(
          "The Workload API source of {} is shared and keeps the backoff it started with",
          settings.socket());
    }
    if (entry == null) {
      entry = new Entry(X509Source.start(settings, clock));
      if (jmxDomain != null) {
        try {
          entry.mbean = entry.source.registerMBean(jmxDomain);
        } catch (JMException e) {
          LOG.warn("Cannot register the MBean of the source of {}", settings.socket(), e);
        }
      }
      sources.put(identity, entry);
    }
    entry.references++;
    return new Handle(entry, identity);
  }

  /** How many sources are running. */
  public synchronized int size() {
    return sources.size();
  }

  private synchronized void release(Entry entry, X509Source.Identity identity) {
    if (--entry.references == 0) {
      sources.remove(identity);
      if (entry.mbean != null) {
        entry.mbean.close();
      }
      entry.source.close();
    }
  }

  private static final class Entry {
    private final X509Source source;
    private GaugeMBean.Registration mbean;
    private int references;

    Entry(X509Source source) {
      this.source = source;
    }
  }

  private final class Handle implements X509MaterialSource {
    private final Entry entry;
    private final X509Source.Identity identity;
    private boolean released;

    Handle(Entry entry, X509Source.Identity identity) {
      this.entry = entry;
      this.identity = identity;
    }

    @Override
    public X509Context current() {
      return entry.source.current();
    }

    @Override
    public X509Context awaitFirst(Duration timeout) throws TimeoutException, InterruptedException {
      return entry.source.awaitFirst(timeout);
    }

    @Override
    public void addListener(Consumer<X509Context> listener) {
      entry.source.addListener(listener);
    }

    @Override
    public void removeListener(Consumer<X509Context> listener) {
      entry.source.removeListener(listener);
    }

    @Override
    public void close() {
      synchronized (X509SourceRegistry.this) {
        if (released) {
          return;
        }
        released = true;
      }
      release(entry, identity);
    }
  }
}
