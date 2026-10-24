package io.github.bpalermo.kafka.spiffe.x509;

import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The trust bundles of a snapshot, one per trust domain: the workload's own and those federated
 * with it. Immutable.
 */
public final class BundleSet {

  private final Map<TrustDomain, X509Bundle> bundles;

  private BundleSet(Map<TrustDomain, X509Bundle> bundles) {
    this.bundles = Map.copyOf(bundles);
  }

  /**
   * The set of {@code bundles}.
   *
   * @throws IllegalArgumentException if two are of the same trust domain
   */
  public static BundleSet of(Collection<X509Bundle> bundles) {
    Map<TrustDomain, X509Bundle> byDomain = new LinkedHashMap<>();
    for (X509Bundle bundle : bundles) {
      if (byDomain.putIfAbsent(bundle.trustDomain(), bundle) != null) {
        throw new IllegalArgumentException(
            "two bundles of the trust domain " + bundle.trustDomain());
      }
    }
    return new BundleSet(byDomain);
  }

  /** The bundle of {@code trustDomain}, if the set has one. */
  public Optional<X509Bundle> get(TrustDomain trustDomain) {
    return Optional.ofNullable(bundles.get(trustDomain));
  }

  /** The trust domains the set has bundles of. */
  public Set<TrustDomain> trustDomains() {
    return bundles.keySet();
  }

  /** Every bundle. */
  public Collection<X509Bundle> bundles() {
    return bundles.values();
  }

  /**
   * The bundles of {@code trustDomains} only, for a workload that trusts fewer trust domains than
   * its agent delivers. A trust domain named but absent is simply not there.
   */
  public BundleSet retainOnly(Set<TrustDomain> trustDomains) {
    Map<TrustDomain, X509Bundle> kept = new LinkedHashMap<>();
    bundles.forEach(
        (domain, bundle) -> {
          if (trustDomains.contains(domain)) {
            kept.put(domain, bundle);
          }
        });
    return new BundleSet(kept);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof BundleSet other && bundles.equals(other.bundles);
  }

  @Override
  public int hashCode() {
    return bundles.hashCode();
  }

  @Override
  public String toString() {
    return "BundleSet" + bundles.values();
  }
}
