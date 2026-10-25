package io.github.bpalermo.kafka.spiffe.kafka;

import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import java.util.EnumMap;
import java.util.Map;

/**
 * The peers the engine factories of this process accepted and refused in their handshakes, by the
 * side the process took and the reason (design, section 7): the numbers of the {@code
 * type=peer-verification} MBeans, readable without JMX, so also with {@code
 * spiffe.metrics.jmx.enabled=false}.
 *
 * <p>The counts are kept per JMX domain, as the factories keep them: every factory configured with
 * the same {@code spiffe.metrics.jmx.domain}, or every one with the MBeans off, counts in the same
 * place, for the life of the process, so they only grow.
 *
 * <p>Public API (design, section 3).
 */
public final class PeerVerificationStats {

  /**
   * One side's counts, as they stood when read.
   *
   * @param accepted peers accepted
   * @param rejected peers refused, by reason; every reason is present
   */
  public record Side(long accepted, Map<PeerRejectedException.Reason, Long> rejected) {

    public Side {
      rejected = Map.copyOf(rejected);
    }

    /** Peers refused, for any reason. */
    public long rejectedTotal() {
      long total = 0;
      for (long count : rejected.values()) {
        total += count;
      }
      return total;
    }
  }

  private final PeerVerification counts;

  private PeerVerificationStats(PeerVerification counts) {
    this.counts = counts;
  }

  /**
   * The counts of the factories whose JMX domain is {@code jmxDomain}: {@link
   * SpiffeConfig#jmxDomain()} of their configuration, null when the MBeans are off. A live view:
   * each read returns the counts of that moment.
   */
  public static PeerVerificationStats of(String jmxDomain) {
    return new PeerVerificationStats(PeerVerification.of(jmxDomain));
  }

  /** Handshakes in which this process was the server: the peers are clients. */
  public Side server() {
    return side(true);
  }

  /** Handshakes in which this process was the client: the peers are servers. */
  public Side client() {
    return side(false);
  }

  private Side side(boolean server) {
    PeerVerification.Counts side = counts.side(server);
    Map<PeerRejectedException.Reason, Long> rejected =
        new EnumMap<>(PeerRejectedException.Reason.class);
    for (PeerRejectedException.Reason reason : PeerRejectedException.Reason.values()) {
      rejected.put(reason, side.rejected(reason));
    }
    return new Side(side.accepted(), rejected);
  }
}
