package io.github.bpalermo.kafka.spiffe.id;

/**
 * A SPIFFE trust domain: the name that SPIFFE IDs and trust bundles belong to, such as {@code
 * example.org}.
 *
 * <p>Parsing is strict, as the SPIFFE ID specification asks: lowercase letters, digits, dots,
 * hyphens and underscores only, at most {@value #MAX_LENGTH} characters. An uppercase name is
 * refused rather than lowered, so that a name that looks like another never names the same trust
 * domain.
 *
 * @param name the trust domain's name, without a scheme
 */
public record TrustDomain(String name) {

  /** The longest trust domain name the specification allows. */
  public static final int MAX_LENGTH = 255;

  public TrustDomain {
    String problem = problem(name);
    if (problem != null) {
      throw new IllegalArgumentException("invalid trust domain \"" + name + "\": " + problem);
    }
  }

  /**
   * The trust domain named {@code name}, such as {@code example.org}.
   *
   * @throws IllegalArgumentException if it is not a valid trust domain name
   */
  public static TrustDomain parse(String name) {
    return new TrustDomain(name);
  }

  /** The SPIFFE ID of the trust domain itself, {@code spiffe://<name>}. */
  public SpiffeId id() {
    return new SpiffeId(this, "");
  }

  @Override
  public String toString() {
    return name;
  }

  /** What is wrong with a trust domain name, or null if nothing is. */
  static String problem(String name) {
    if (name == null || name.isEmpty()) {
      return "it is empty";
    }
    if (name.length() > MAX_LENGTH) {
      return "it is longer than " + MAX_LENGTH + " characters";
    }
    if (name.startsWith(SpiffeId.SCHEME)) {
      return "a trust domain is named without the " + SpiffeId.SCHEME + " scheme";
    }
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (!allowed(c)) {
        return "character '" + c + "' at " + i + " is not one of a-z, 0-9, '.', '-', '_'";
      }
    }
    return null;
  }

  private static boolean allowed(char c) {
    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_';
  }
}
