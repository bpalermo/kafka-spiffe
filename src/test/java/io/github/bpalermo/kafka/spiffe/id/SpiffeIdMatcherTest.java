package io.github.bpalermo.kafka.spiffe.id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SpiffeIdMatcherTest {

  private static boolean matches(SpiffeIdMatcher matcher, String id) {
    return matcher.matches(SpiffeId.parse(id));
  }

  @Test
  void nothingMatchesAnEmptyMatcher() {
    SpiffeIdMatcher none = SpiffeIdMatcher.none();
    assertTrue(none.isEmpty());
    assertFalse(matches(none, "spiffe://example.org/a"));
    assertEquals("nothing", none.toString());
  }

  @Test
  void exactIdsMatchOnlyThemselves() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/broker"));
    assertTrue(matches(matcher, "spiffe://example.org/broker"));
    assertFalse(matches(matcher, "spiffe://example.org/broker/x"));
    assertFalse(matches(matcher, "spiffe://example.org/Broker"));
    assertFalse(matches(matcher, "spiffe://example.com/broker"));
  }

  @Test
  void aTrustDomainMatchesAllItsIds() {
    SpiffeIdMatcher matcher = SpiffeIdMatcher.none().withTrustDomains(List.of("example.org"));
    assertTrue(matches(matcher, "spiffe://example.org"));
    assertTrue(matches(matcher, "spiffe://example.org/a/b/c"));
    assertFalse(matches(matcher, "spiffe://k8s.example.org/a"));
    assertFalse(matches(matcher, "spiffe://example.org.evil/a"));
  }

  @Test
  void oneStarIsOneSegment() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none().withPatterns(List.of("spiffe://example.org/ns/*/sa/api"));
    assertTrue(matches(matcher, "spiffe://example.org/ns/payments/sa/api"));
    assertFalse(matches(matcher, "spiffe://example.org/ns/a/b/sa/api"));
    assertFalse(matches(matcher, "spiffe://example.org/ns/payments/sa/api/x"));
    assertFalse(matches(matcher, "spiffe://example.org/ns/sa/api"));
  }

  @Test
  void twoStarsAreOneOrMoreTrailingSegments() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none().withPatterns(List.of("spiffe://k8s.example.org/ns/*/sa/**"));
    assertTrue(matches(matcher, "spiffe://k8s.example.org/ns/a/sa/b"));
    assertTrue(matches(matcher, "spiffe://k8s.example.org/ns/a/sa/b/c/d"));
    assertFalse(matches(matcher, "spiffe://k8s.example.org/ns/a/sa"));
    assertFalse(matches(matcher, "spiffe://k8s.example.org/ns/a"));
    assertFalse(matches(matcher, "spiffe://example.org/ns/a/sa/b"));
  }

  @Test
  void twoStarsAloneAreEveryIdButTheTrustDomains() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none().withPatterns(List.of("spiffe://example.org/**"));
    assertTrue(matches(matcher, "spiffe://example.org/a"));
    assertFalse(matches(matcher, "spiffe://example.org"));
  }

  @Test
  void aPatternWithoutWildcardsIsExact() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none().withPatterns(List.of("spiffe://example.org/a/b"));
    assertTrue(matches(matcher, "spiffe://example.org/a/b"));
    assertFalse(matches(matcher, "spiffe://example.org/a"));
    assertFalse(matches(matcher, "spiffe://example.org/a/b/c"));
  }

  @Test
  void refusesInvalidPatterns() {
    for (String pattern :
        List.of(
            "example.org/*",
            "spiffe://Example.org/*",
            "spiffe://*.example.org/a",
            "spiffe://example.org/**/a",
            "spiffe://example.org/web-*",
            "spiffe://example.org/a*b",
            "spiffe://example.org/***",
            "spiffe://example.org//a",
            "spiffe://example.org/a/",
            "spiffe://example.org/../a",
            "spiffe://example.org/a?b")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> SpiffeIdMatcher.none().withPatterns(List.of(pattern)),
          pattern);
    }
  }

  @Test
  void refusesInvalidIdsAndTrustDomains() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SpiffeIdMatcher.none().withIds(List.of("spiffe://example.org/a/")));
    assertThrows(
        IllegalArgumentException.class,
        () -> SpiffeIdMatcher.none().withTrustDomains(List.of("spiffe://example.org")));
  }

  @Test
  void combinesAllThree() {
    SpiffeIdMatcher matcher =
        SpiffeIdMatcher.none()
            .withIds(List.of("spiffe://example.org/broker"))
            .withTrustDomains(List.of("clients.example.org"))
            .withPatterns(List.of("spiffe://k8s.example.org/ns/*/sa/**"));
    assertTrue(matches(matcher, "spiffe://example.org/broker"));
    assertTrue(matches(matcher, "spiffe://clients.example.org/x"));
    assertTrue(matches(matcher, "spiffe://k8s.example.org/ns/a/sa/b"));
    assertFalse(matches(matcher, "spiffe://example.org/other"));
    assertEquals(
        Set.of(
            TrustDomain.parse("example.org"),
            TrustDomain.parse("clients.example.org"),
            TrustDomain.parse("k8s.example.org")),
        matcher.trustDomains());
  }

  @Test
  void isAValue() {
    SpiffeIdMatcher a =
        SpiffeIdMatcher.none()
            .withIds(List.of("spiffe://example.org/a"))
            .withPatterns(List.of("spiffe://example.org/*", "spiffe://example.org/x/**"));
    SpiffeIdMatcher b =
        SpiffeIdMatcher.none()
            .withPatterns(List.of("spiffe://example.org/x/**", "spiffe://example.org/*"))
            .withIds(List.of("spiffe://example.org/a"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(
        "IDs [spiffe://example.org/a], patterns [spiffe://example.org/*,"
            + " spiffe://example.org/x/**]",
        a.toString());
  }
}
