package io.github.bpalermo.kafka.spiffe.id;

import java.nio.charset.StandardCharsets;

/**
 * A SPIFFE ID, {@code spiffe://<trust domain><path>}, parsed strictly as the SPIFFE ID
 * specification requires.
 *
 * <p>Only the canonical form is accepted: the scheme in lowercase, a valid {@link TrustDomain}, and
 * a path of segments of {@code [A-Za-z0-9._-]}, none empty, {@code .} or {@code ..}, with no
 * trailing slash. A port, user information, a query, a fragment and percent-encoding are all
 * refused, since none of their characters may appear. An ID that is not canonical is refused rather
 * than normalised, so that it can never be taken for an ID an allow-list or an ACL names.
 *
 * @param trustDomain the trust domain the ID belongs to
 * @param path the path, empty for the trust domain's own ID, otherwise starting with {@code /}
 *     <p>Public API (design, section 3).
 */
public record SpiffeId(TrustDomain trustDomain, String path) {

  /** The scheme every SPIFFE ID starts with, followed by the trust domain. */
  public static final String SCHEME = "spiffe://";

  /** The longest SPIFFE ID the specification allows, in bytes. */
  public static final int MAX_LENGTH = 2048;

  public SpiffeId {
    if (trustDomain == null) {
      throw new IllegalArgumentException("a SPIFFE ID needs a trust domain");
    }
    String problem = pathProblem(path);
    if (problem != null) {
      throw new IllegalArgumentException("invalid SPIFFE ID path \"" + path + "\": " + problem);
    }
    int length = SCHEME.length() + trustDomain.name().length() + path.length();
    if (length > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "a SPIFFE ID is at most " + MAX_LENGTH + " bytes, this one is " + length);
    }
  }

  /**
   * Parses a SPIFFE ID, such as {@code spiffe://example.org/ns/payments/sa/api}.
   *
   * @throws IllegalArgumentException if {@code id} is not a SPIFFE ID in canonical form
   */
  public static SpiffeId parse(String id) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("a SPIFFE ID cannot be empty");
    }
    if (id.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH) {
      throw new IllegalArgumentException("a SPIFFE ID is at most " + MAX_LENGTH + " bytes");
    }
    if (!id.startsWith(SCHEME)) {
      throw new IllegalArgumentException(
          "invalid SPIFFE ID \"" + id + "\": it does not start with " + SCHEME);
    }
    String rest = id.substring(SCHEME.length());
    int slash = rest.indexOf('/');
    String domain = slash < 0 ? rest : rest.substring(0, slash);
    String path = slash < 0 ? "" : rest.substring(slash);
    String problem = TrustDomain.problem(domain);
    if (problem != null) {
      throw new IllegalArgumentException(
          "invalid SPIFFE ID \"" + id + "\": its trust domain is invalid, " + problem);
    }
    problem = pathProblem(path);
    if (problem != null) {
      throw new IllegalArgumentException(
          "invalid SPIFFE ID \"" + id + "\": its path is invalid, " + problem);
    }
    return new SpiffeId(new TrustDomain(domain), path);
  }

  /** Whether this is a trust domain's own ID, with an empty path. */
  public boolean isTrustDomainId() {
    return path.isEmpty();
  }

  /** The ID as a string, {@code spiffe://<trust domain><path>}. */
  @Override
  public String toString() {
    return SCHEME + trustDomain.name() + path;
  }

  /** What is wrong with a path, or null if nothing is. */
  static String pathProblem(String path) {
    if (path == null) {
      return "it is null";
    }
    if (path.isEmpty()) {
      return null;
    }
    if (path.charAt(0) != '/') {
      return "it does not start with '/'";
    }
    int start = 1;
    while (true) {
      int end = path.indexOf('/', start);
      String segment = end < 0 ? path.substring(start) : path.substring(start, end);
      String problem = segmentProblem(segment);
      if (problem != null) {
        return problem;
      }
      if (end < 0) {
        return null;
      }
      start = end + 1;
    }
  }

  /** What is wrong with one path segment, or null if nothing is. */
  static String segmentProblem(String segment) {
    if (segment.isEmpty()) {
      return "it has an empty segment (\"//\" or a trailing '/')";
    }
    if (segment.equals(".") || segment.equals("..")) {
      return "it has a \"" + segment + "\" segment";
    }
    for (int i = 0; i < segment.length(); i++) {
      char c = segment.charAt(i);
      if (!allowedInPath(c)) {
        return "character '" + c + "' is not one of A-Z, a-z, 0-9, '.', '-', '_'";
      }
    }
    return null;
  }

  private static boolean allowedInPath(char c) {
    return (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9')
        || c == '.'
        || c == '-'
        || c == '_';
  }
}
