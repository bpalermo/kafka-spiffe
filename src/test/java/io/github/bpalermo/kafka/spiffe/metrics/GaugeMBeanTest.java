package io.github.bpalermo.kafka.spiffe.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

class GaugeMBeanTest {

  @Test
  void readsAttributesWhenAskedAndUnregistersOnClose() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = new ObjectName("kafka.spiffe.test:type=gauge,socket=\"/tmp/a.sock\"");
    AtomicLong count = new AtomicLong(1);
    try (GaugeMBean.Registration registration =
        GaugeMBean.builder("a test")
            .longAttribute("updates-total", "updates", count::get)
            .booleanAttribute("connected", "connected", () -> true)
            .stringAttribute("bundle-trust-domains", "domains", () -> "a,b")
            .register(name)) {
      assertEquals(1L, server.getAttribute(name, "updates-total"));
      count.set(5);
      assertEquals(5L, server.getAttribute(name, "updates-total"));
      assertEquals(true, server.getAttribute(name, "connected"));
      assertEquals("a,b", server.getAttribute(name, "bundle-trust-domains"));
      assertThrows(AttributeNotFoundException.class, () -> server.getAttribute(name, "nope"));
      assertEquals(
          3,
          server
              .getAttributes(
                  name, new String[] {"updates-total", "connected", "bundle-trust-domains", "nope"})
              .size());
      MBeanAttributeInfo[] infos = server.getMBeanInfo(name).getAttributes();
      assertTrue(
          Arrays.stream(infos)
              .anyMatch(
                  i -> i.getName().equals("updates-total") && i.isReadable() && !i.isWritable()));
      assertEquals(registration.name(), name);
    }
    assertFalse(server.isRegistered(name));
  }

  @Test
  void replacesAnMBeanOfTheSameName() throws Exception {
    ObjectName name = new ObjectName("kafka.spiffe.test:type=gauge,name=replace");
    GaugeMBean.Registration first =
        GaugeMBean.builder("first").longAttribute("v", "v", () -> 1L).register(name);
    GaugeMBean.Registration second =
        GaugeMBean.builder("second").longAttribute("v", "v", () -> 2L).register(name);
    assertEquals(2L, ManagementFactory.getPlatformMBeanServer().getAttribute(name, "v"));
    first.close();
    second.close();
    assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(name));
  }
}
