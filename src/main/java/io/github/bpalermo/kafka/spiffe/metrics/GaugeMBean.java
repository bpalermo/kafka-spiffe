package io.github.bpalermo.kafka.spiffe.metrics;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A read-only MBean whose attributes are read when asked for, named as the design names them
 * ({@code updates-total}, not {@code UpdatesTotal}), so that JMX exporters see the same names
 * everywhere.
 */
public final class GaugeMBean implements DynamicMBean {

  private final String description;
  private final Map<String, Attribute> attributes;

  /** An attribute: its name, its JMX type, and how to read it. */
  private record Attribute(String name, Class<?> type, String description, Supplier<?> value) {}

  private GaugeMBean(String description, Map<String, Attribute> attributes) {
    this.description = description;
    this.attributes = Map.copyOf(attributes);
  }

  /** Builds the attributes of an MBean. */
  public static Builder builder(String description) {
    return new Builder(description);
  }

  /** Builds a {@link GaugeMBean}. */
  public static final class Builder {
    private final String description;
    private final Map<String, Attribute> attributes = new LinkedHashMap<>();

    private Builder(String description) {
      this.description = description;
    }

    public Builder longAttribute(String name, String description, Supplier<Long> value) {
      attributes.put(name, new Attribute(name, Long.class, description, value));
      return this;
    }

    public Builder booleanAttribute(String name, String description, Supplier<Boolean> value) {
      attributes.put(name, new Attribute(name, Boolean.class, description, value));
      return this;
    }

    public Builder stringAttribute(String name, String description, Supplier<String> value) {
      attributes.put(name, new Attribute(name, String.class, description, value));
      return this;
    }

    /**
     * Registers the MBean under {@code name} in the platform MBean server, replacing one already
     * there, and returns what unregisters it.
     *
     * @throws JMException if the name is invalid or the server refuses the MBean
     */
    public Registration register(ObjectName name) throws JMException {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      GaugeMBean mbean = new GaugeMBean(description, attributes);
      synchronized (OWNERS) {
        if (server.isRegistered(name)) {
          server.unregisterMBean(name);
        }
        server.registerMBean(mbean, name);
        OWNERS.put(name, mbean);
      }
      return new Registration(server, name, mbean);
    }
  }

  /**
   * A registered MBean; closing it unregisters it, unless another registration has since replaced
   * it under the same name, whose MBean stays. Closing twice does nothing more.
   */
  public static final class Registration implements AutoCloseable {
    private final MBeanServer server;
    private final ObjectName name;
    private final GaugeMBean mbean;

    private Registration(MBeanServer server, ObjectName name, GaugeMBean mbean) {
      this.server = server;
      this.name = name;
      this.mbean = mbean;
    }

    public ObjectName name() {
      return name;
    }

    @Override
    public void close() {
      synchronized (OWNERS) {
        if (OWNERS.get(name) != mbean) {
          return;
        }
        OWNERS.remove(name);
        try {
          if (server.isRegistered(name)) {
            server.unregisterMBean(name);
          }
        } catch (JMException e) {
          // Already gone: nothing to undo.
        }
      }
    }
  }

  /** Which MBean each name was last registered with, so a stale handle leaves its successor. */
  private static final Map<ObjectName, GaugeMBean> OWNERS = new HashMap<>();

  @Override
  public Object getAttribute(String name) throws AttributeNotFoundException {
    Attribute attribute = attributes.get(name);
    if (attribute == null) {
      throw new AttributeNotFoundException(name);
    }
    return attribute.value().get();
  }

  @Override
  public void setAttribute(javax.management.Attribute attribute) {
    throw new UnsupportedOperationException("read-only: " + attribute.getName());
  }

  @Override
  public AttributeList getAttributes(String[] names) {
    AttributeList list = new AttributeList();
    for (String name : names) {
      Attribute attribute = attributes.get(name);
      if (attribute != null) {
        list.add(new javax.management.Attribute(name, attribute.value().get()));
      }
    }
    return list;
  }

  @Override
  public AttributeList setAttributes(AttributeList list) {
    return new AttributeList();
  }

  @Override
  public Object invoke(String action, Object[] params, String[] signature) {
    throw new UnsupportedOperationException("no operations: " + action);
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    MBeanAttributeInfo[] infos =
        attributes.values().stream()
            .map(
                a ->
                    new MBeanAttributeInfo(
                        a.name(), a.type().getName(), a.description(), true, false, false))
            .toArray(MBeanAttributeInfo[]::new);
    return new MBeanInfo(GaugeMBean.class.getName(), description, infos, null, null, null);
  }
}
