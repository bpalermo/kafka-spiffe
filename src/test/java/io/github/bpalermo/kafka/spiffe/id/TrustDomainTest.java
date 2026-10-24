package io.github.bpalermo.kafka.spiffe.id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class TrustDomainTest {

  @TestFactory
  Stream<DynamicTest> acceptsValidNames() {
    return Stream.of("example.org", "k8s.example.org", "a", "a-b_c.0-9", "1.2.3.4")
        .map(name -> dynamicTest(name, () -> assertEquals(name, TrustDomain.parse(name).name())));
  }

  @TestFactory
  Stream<DynamicTest> refusesInvalidNames() {
    return Stream.of(
            "",
            "Example.org",
            "spiffe://example.org",
            "example.org/path",
            "example.org:8443",
            "user@example.org",
            "exa mple.org",
            "exämple.org",
            "example.org ")
        .map(
            name ->
                dynamicTest(
                    "\"" + name + "\"",
                    () ->
                        assertThrows(
                            IllegalArgumentException.class, () -> TrustDomain.parse(name))));
  }

  @Test
  void refusesNull() {
    assertThrows(IllegalArgumentException.class, () -> TrustDomain.parse(null));
  }

  @Test
  void refusesANameLongerThan255() {
    String atLimit = "a".repeat(TrustDomain.MAX_LENGTH);
    assertEquals(atLimit, TrustDomain.parse(atLimit).name());
    assertThrows(IllegalArgumentException.class, () -> TrustDomain.parse(atLimit + "a"));
  }

  @Test
  void itsIdIsTheSchemeAndTheName() {
    assertEquals("spiffe://example.org", TrustDomain.parse("example.org").id().toString());
  }
}
