package io.github.bpalermo.kafka.spiffe.id;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Which SPIFFE IDs are allowed: any of a set of exact IDs, any ID of a set of trust domains, and
 * any ID a path pattern matches.
 *
 * <p>A pattern is a SPIFFE ID whose path segments may be wildcards: {@code *} matches exactly one
 * segment, and {@code **}, only as the last segment, matches one or more. {@code
 * spiffe://example.org/ns/*}{@code /sa/**} matches {@code spiffe://example.org/ns/a/sa/b} and
 * {@code spiffe://example.org/ns/a/sa/b/c}, but neither {@code spiffe://example.org/ns/a/sa} nor an
 * ID of another trust domain. A wildcard is a whole segment: {@code web-*} is refused, not matched
 * literally. The trust domain of a pattern is always exact.
 *
 * <p>A matcher with nothing in it matches nothing: there is no implicit "anything a bundle vouches
 * for".
 *
 * <p>Public API (design, section 3).
 */
public final class SpiffeIdMatcher {

  private static final String ONE = "*";
  private static final String REST = "**";

  private final Set<SpiffeId> ids;
  private final Set<TrustDomain> trustDomains;
  private final List<Pattern> patterns;

  private SpiffeIdMatcher(
      Set<SpiffeId> ids, Set<TrustDomain> trustDomains, List<Pattern> patterns) {
    this.ids = Set.copyOf(ids);
    this.trustDomains = Set.copyOf(trustDomains);
    this.patterns = List.copyOf(patterns);
  }

  /** A matcher that matches nothing, to be extended with the {@code with} methods. */
  public static SpiffeIdMatcher none() {
    return new SpiffeIdMatcher(Set.of(), Set.of(), List.of());
  }

  /**
   * This matcher, also matching each of {@code ids} exactly.
   *
   * @throws IllegalArgumentException if one is not a SPIFFE ID
   */
  public SpiffeIdMatcher withIds(Collection<String> ids) {
    Set<SpiffeId> more = new LinkedHashSet<>(this.ids);
    for (String id : ids) {
      more.add(SpiffeId.parse(id));
    }
    return new SpiffeIdMatcher(more, trustDomains, patterns);
  }

  /**
   * This matcher, also matching every ID of each of {@code trustDomains}.
   *
   * @throws IllegalArgumentException if one is not a trust domain name
   */
  public SpiffeIdMatcher withTrustDomains(Collection<String> trustDomains) {
    Set<TrustDomain> more = new LinkedHashSet<>(this.trustDomains);
    for (String name : trustDomains) {
      more.add(TrustDomain.parse(name));
    }
    return new SpiffeIdMatcher(ids, more, patterns);
  }

  /**
   * This matcher, also matching what each of {@code patterns} matches.
   *
   * @throws IllegalArgumentException if one is not a valid pattern
   */
  public SpiffeIdMatcher withPatterns(Collection<String> patterns) {
    List<Pattern> more = new ArrayList<>(this.patterns);
    for (String pattern : patterns) {
      Pattern parsed = Pattern.parse(pattern);
      if (!more.contains(parsed)) {
        more.add(parsed);
      }
    }
    return new SpiffeIdMatcher(ids, trustDomains, more);
  }

  /** Whether {@code id} is allowed. */
  public boolean matches(SpiffeId id) {
    if (ids.contains(id) || trustDomains.contains(id.trustDomain())) {
      return true;
    }
    for (Pattern pattern : patterns) {
      if (pattern.matches(id)) {
        return true;
      }
    }
    return false;
  }

  /** Whether the matcher matches nothing at all. */
  public boolean isEmpty() {
    return ids.isEmpty() && trustDomains.isEmpty() && patterns.isEmpty();
  }

  /**
   * The trust domains whose IDs can match: those named, and those of the exact IDs and patterns.
   * Anything outside them is refused before its chain is looked at.
   */
  public Set<TrustDomain> trustDomains() {
    Set<TrustDomain> all = new LinkedHashSet<>(trustDomains);
    ids.forEach(id -> all.add(id.trustDomain()));
    patterns.forEach(pattern -> all.add(pattern.trustDomain()));
    return Set.copyOf(all);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SpiffeIdMatcher other
        && ids.equals(other.ids)
        && trustDomains.equals(other.trustDomains)
        && Set.copyOf(patterns).equals(Set.copyOf(other.patterns));
  }

  @Override
  public int hashCode() {
    return ids.hashCode() * 31 * 31
        + trustDomains.hashCode() * 31
        + Set.copyOf(patterns).hashCode();
  }

  @Override
  public String toString() {
    if (isEmpty()) {
      return "nothing";
    }
    List<String> parts = new ArrayList<>();
    if (!ids.isEmpty()) {
      parts.add("IDs " + ids);
    }
    if (!trustDomains.isEmpty()) {
      parts.add("trust domains " + trustDomains);
    }
    if (!patterns.isEmpty()) {
      parts.add("patterns " + patterns);
    }
    return String.join(", ", parts);
  }

  /** A SPIFFE ID with wildcard segments. */
  private record Pattern(TrustDomain trustDomain, List<String> segments) {

    static Pattern parse(String pattern) {
      if (pattern == null || !pattern.startsWith(SpiffeId.SCHEME)) {
        throw new IllegalArgumentException(
            "invalid SPIFFE ID pattern \""
                + pattern
                + "\": it does not start with "
                + SpiffeId.SCHEME);
      }
      String rest = pattern.substring(SpiffeId.SCHEME.length());
      int slash = rest.indexOf('/');
      String domain = slash < 0 ? rest : rest.substring(0, slash);
      String problem = TrustDomain.problem(domain);
      if (problem != null) {
        throw new IllegalArgumentException(
            "invalid SPIFFE ID pattern \""
                + pattern
                + "\": its trust domain is invalid, "
                + problem);
      }
      List<String> segments = new ArrayList<>();
      if (slash >= 0) {
        segments.addAll(Arrays.asList(rest.substring(slash + 1).split("/", -1)));
      }
      for (int i = 0; i < segments.size(); i++) {
        String segment = segments.get(i);
        if (segment.equals(REST) && i != segments.size() - 1) {
          throw new IllegalArgumentException(
              "invalid SPIFFE ID pattern \""
                  + pattern
                  + "\": ** is allowed only as the last segment");
        }
        if (segment.equals(ONE) || segment.equals(REST)) {
          continue;
        }
        if (segment.contains(ONE)) {
          throw new IllegalArgumentException(
              "invalid SPIFFE ID pattern \""
                  + pattern
                  + "\": a wildcard must be a whole segment, not part of \""
                  + segment
                  + "\"");
        }
        problem = SpiffeId.segmentProblem(segment);
        if (problem != null) {
          throw new IllegalArgumentException(
              "invalid SPIFFE ID pattern \"" + pattern + "\": its path is invalid, " + problem);
        }
      }
      return new Pattern(new TrustDomain(domain), List.copyOf(segments));
    }

    boolean matches(SpiffeId id) {
      if (!trustDomain.equals(id.trustDomain())) {
        return false;
      }
      List<String> actual =
          id.path().isEmpty() ? List.of() : List.of(id.path().substring(1).split("/", -1));
      for (int i = 0; i < segments.size(); i++) {
        String segment = segments.get(i);
        if (segment.equals(REST)) {
          return actual.size() > i;
        }
        if (i >= actual.size() || !(segment.equals(ONE) || segment.equals(actual.get(i)))) {
          return false;
        }
      }
      return actual.size() == segments.size();
    }

    @Override
    public String toString() {
      return SpiffeId.SCHEME
          + trustDomain.name()
          + (segments.isEmpty() ? "" : "/" + String.join("/", segments));
    }
  }
}
