package io.github.bpalermo.kafka.spiffe.id;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PlaceholderTest {
  @Test
  void namesItsPackage() {
    assertEquals(Placeholder.class.getPackageName(), Placeholder.PACKAGE);
  }
}
