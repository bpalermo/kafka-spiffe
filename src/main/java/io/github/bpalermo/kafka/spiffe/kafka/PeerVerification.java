package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.metrics.GaugeMBean;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.JMException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The process's count of the peers its handshakes accepted and refused, by the side this process
 * took and the reason (design, section 7), and a log of the refusals at a limited rate.
 *
 * <p>One per JMX domain, shared by every factory of the process that uses it, and kept for the life
 * of the process, so the counts only grow. While a factory holds it, it is registered as two
 * MBeans, {@code <domain>:type=peer-verification,mode=server} (this process verified a client) and
 * {@code mode=client} (it verified a broker). Without a domain ({@code
 * spiffe.metrics.jmx.enabled=false}) it counts and logs, and registers nothing.
 */
final class PeerVerification implements SpiffeTrustManager.Observer {

  private static final Logger LOG = LoggerFactory.getLogger(PeerVerification.class);

  /** A peer can cause a refusal at will, so their log lines are limited. */
  private static final LogLimiter REFUSALS = new LogLimiter(10, 60_000);

  /** By JMX domain; "" for none. */
  private static final Map<String, PeerVerification> BY_DOMAIN = new HashMap<>();

  /** One side's counts. */
  static final class Counts {
    private final AtomicLong accepted = new AtomicLong();
    private final Map<PeerRejectedException.Reason, AtomicLong> rejected =
        new EnumMap<>(PeerRejectedException.Reason.class);

    private Counts() {
      for (PeerRejectedException.Reason reason : PeerRejectedException.Reason.values()) {
        rejected.put(reason, new AtomicLong());
      }
    }

    long accepted() {
      return accepted.get();
    }

    long rejected(PeerRejectedException.Reason reason) {
      return rejected.get(reason).get();
    }

    long rejected() {
      long total = 0;
      for (AtomicLong count : rejected.values()) {
        total += count.get();
      }
      return total;
    }
  }

  private final String domain;
  private final Counts server = new Counts();
  private final Counts client = new Counts();
  private int users;
  private final List<GaugeMBean.Registration> registrations = new ArrayList<>();

  private PeerVerification(String domain) {
    this.domain = domain;
  }

  /**
   * The counts of {@code domain} (null or empty for none), registered as MBeans until {@link
   * #release()} has been called once for each call of this.
   */
  static PeerVerification acquire(String domain) {
    // An empty domain is none, as X509SourceRegistry.shared reads it.
    String named = domain == null || domain.isEmpty() ? null : domain;
    synchronized (BY_DOMAIN) {
      PeerVerification verification =
          BY_DOMAIN.computeIfAbsent(named == null ? "" : named, d -> new PeerVerification(named));
      if (verification.users++ == 0 && verification.domain != null) {
        verification.register();
      }
      return verification;
    }
  }

  /** Gives back what {@link #acquire} gave; the last release unregisters the MBeans. */
  void release() {
    synchronized (BY_DOMAIN) {
      if (users == 0) {
        return;
      }
      if (--users == 0) {
        registrations.forEach(GaugeMBean.Registration::close);
        registrations.clear();
      }
    }
  }

  /** The counts of handshakes in which this process was the server, or the client. */
  Counts side(boolean server) {
    return server ? this.server : client;
  }

  @Override
  public void accepted(boolean peerIsServer, SpiffeId id) {
    side(!peerIsServer).accepted.incrementAndGet();
  }

  @Override
  public void rejected(boolean peerIsServer, PeerRejectedException rejection) {
    side(!peerIsServer).rejected.get(rejection.reason()).incrementAndGet();
    long suppressed = REFUSALS.acquire();
    if (suppressed >= 0) {
      LOG.warn(
          "Refused a TLS {} in the handshake ({}): {}{}",
          peerIsServer ? "server" : "client",
          name(rejection.reason()),
          rejection.getMessage(),
          suppressed > 0 ? " (" + suppressed + " refusals not logged before this one)" : "");
    }
  }

  /** {@code ID_NOT_ALLOWED} as {@code id-not-allowed}, as the attribute names spell it. */
  static String name(PeerRejectedException.Reason reason) {
    return reason.name().toLowerCase(Locale.ROOT).replace('_', '-');
  }

  /** The MBean of one side. */
  static ObjectName objectName(String domain, boolean server) throws JMException {
    return new ObjectName(
        domain + ":type=peer-verification,mode=" + (server ? "server" : "client"));
  }

  private void register() {
    for (boolean isServer : new boolean[] {true, false}) {
      Counts counts = side(isServer);
      GaugeMBean.Builder mbean =
          GaugeMBean.builder(
                  "Peers this process's handshakes verified as the "
                      + (isServer ? "server" : "client"))
              .longAttribute("accepted-total", "Peers accepted", counts::accepted)
              .longAttribute("rejected-total", "Peers refused, for any reason", counts::rejected);
      for (PeerRejectedException.Reason reason : PeerRejectedException.Reason.values()) {
        mbean.longAttribute(
            "rejected-" + name(reason) + "-total",
            "Peers refused: " + name(reason),
            () -> counts.rejected(reason));
      }
      try {
        registrations.add(mbean.register(objectName(domain, isServer)));
      } catch (JMException e) {
        LOG.warn("Cannot register the peer verification MBean in {}: {}", domain, e.toString());
      }
    }
  }
}
