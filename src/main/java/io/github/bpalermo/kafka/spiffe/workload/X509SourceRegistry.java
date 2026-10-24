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
  private final Map<X509Source.Settings, Entry> sources = new HashMap<>();

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

  /** The registry of the process. */
  public static X509SourceRegistry shared() {
    return SHARED;
  }

  /**
   * A handle on the source of {@code settings}, started if it is not running. Closing the handle
   * releases it; the source stops when the last handle is closed.
   */
  public synchronized X509MaterialSource acquire(X509Source.Settings settings) {
    Entry entry = sources.get(settings);
    if (entry == null) {
      entry = new Entry(X509Source.start(settings, clock));
      if (jmxDomain != null) {
        try {
          entry.mbean = entry.source.registerMBean(jmxDomain);
        } catch (JMException e) {
          LOG.warn("Cannot register the MBean of the source of {}", settings.socket(), e);
        }
      }
      sources.put(settings, entry);
    }
    entry.references++;
    return new Handle(entry, settings);
  }

  /** How many sources are running. */
  public synchronized int size() {
    return sources.size();
  }

  private synchronized void release(Entry entry, X509Source.Settings settings) {
    if (--entry.references == 0) {
      sources.remove(settings);
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
    private final X509Source.Settings settings;
    private boolean released;

    Handle(Entry entry, X509Source.Settings settings) {
      this.entry = entry;
      this.settings = settings;
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
      release(entry, settings);
    }
  }
}
