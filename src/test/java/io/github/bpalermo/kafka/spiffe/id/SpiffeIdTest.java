package io.github.bpalermo.kafka.spiffe.id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** The SPIFFE ID specification's rules, case by case. */
class SpiffeIdTest {

  @TestFactory
  Stream<DynamicTest> acceptsCanonicalIds() {
    return Stream.of(
            "spiffe://example.org",
            "spiffe://example.org/workload",
            "spiffe://example.org/ns/payments/sa/api",
            "spiffe://k8s.example.org/ns/a/sa/b",
            "spiffe://a-b_c.0-9/path",
            "spiffe://example.org/A-Z/a-z/0-9/._-",
            "spiffe://example.org/.../a",
            "spiffe://example.org/.a/a.",
            "spiffe://1.2.3.4/x")
        .map(id -> dynamicTest(id, () -> assertEquals(id, SpiffeId.parse(id).toString())));
  }

  @TestFactory
  Stream<DynamicTest> refusesWhatIsNotCanonical() {
    return Stream.of(
            "",
            "example.org/workload",
            "spiffe:example.org/workload",
            "spiffe:/example.org/workload",
            "http://example.org/workload",
            "SPIFFE://example.org/workload",
            "Spiffe://example.org/workload",
            "spiffe://",
            "spiffe:///workload",
            "spiffe://Example.org/workload",
            "spiffe://EXAMPLE.ORG",
            "spiffe://example.org/",
            "spiffe://example.org//workload",
            "spiffe://example.org/workload/",
            "spiffe://example.org/./workload",
            "spiffe://example.org/a/../b",
            "spiffe://example.org/a/..",
            "spiffe://example.org/.",
            "spiffe://example.org:8443/workload",
            "spiffe://user@example.org/workload",
            "spiffe://user:password@example.org/workload",
            "spiffe://example.org/workload?query",
            "spiffe://example.org/workload#fragment",
            "spiffe://example.org/work%20load",
            "spiffe://exa%41mple.org/workload",
            "spiffe://example.org/work load",
            "spiffe://exa mple.org/workload",
            "spiffe://example.org/wörkload",
            "spiffe://exämple.org/workload",
            "spiffe://example.org/a\\b",
            "spiffe://example.org/a+b",
            "spiffe://example.org/a~b",
            "spiffe://[::1]/workload",
            " spiffe://example.org/workload",
            "spiffe://example.org/workload ")
        .map(
            id ->
                dynamicTest(
                    "\"" + id + "\"",
                    () -> assertThrows(IllegalArgumentException.class, () -> SpiffeId.parse(id))));
  }

  @Test
  void refusesNull() {
    assertThrows(IllegalArgumentException.class, () -> SpiffeId.parse(null));
  }

  @Test
  void splitsTrustDomainAndPath() {
    SpiffeId id = SpiffeId.parse("spiffe://example.org/ns/payments/sa/api");
    assertEquals(TrustDomain.parse("example.org"), id.trustDomain());
    assertEquals("/ns/payments/sa/api", id.path());
    assertFalse(id.isTrustDomainId());
  }

  @Test
  void aTrustDomainIdHasAnEmptyPath() {
    SpiffeId id = SpiffeId.parse("spiffe://example.org");
    assertTrue(id.isTrustDomainId());
    assertEquals(TrustDomain.parse("example.org").id(), id);
  }

  @Test
  void equalIdsAreEqual() {
    assertEquals(
        SpiffeId.parse("spiffe://example.org/a"), SpiffeId.parse("spiffe://example.org/a"));
    assertEquals(
        SpiffeId.parse("spiffe://example.org/a").hashCode(),
        SpiffeId.parse("spiffe://example.org/a").hashCode());
  }

  @Test
  void pathsAreCaseSensitive() {
    assertFalse(
        SpiffeId.parse("spiffe://example.org/A").equals(SpiffeId.parse("spiffe://example.org/a")));
  }

  @Test
  void refusesAnIdLongerThan2048Bytes() {
    String prefix = "spiffe://example.org/";
    String atLimit = prefix + "a".repeat(SpiffeId.MAX_LENGTH - prefix.length());
    assertEquals(atLimit, SpiffeId.parse(atLimit).toString());
    assertThrows(IllegalArgumentException.class, () -> SpiffeId.parse(atLimit + "a"));
  }

  @Test
  void theConstructorChecksToo() {
    TrustDomain domain = TrustDomain.parse("example.org");
    assertThrows(IllegalArgumentException.class, () -> new SpiffeId(domain, "workload"));
    assertThrows(IllegalArgumentException.class, () -> new SpiffeId(domain, "/a/"));
    assertThrows(IllegalArgumentException.class, () -> new SpiffeId(null, "/a"));
  }

  @Test
  void anErrorSaysWhatIsWrong() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> SpiffeId.parse("spiffe://example.org/a/../b"));
    assertTrue(e.getMessage().contains("\"..\""), e.getMessage());
  }
}
