package kafka.testkit;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Uuid;

/**
 * Kafka 3.3's test kit sets each broker's listener protocols after the test's own settings, and
 * takes a broker's property overrides only through a package-private constructor of {@link
 * BrokerNode}, hence this class in its package (design, section 9).
 */
public final class BrokerNodes {

  private BrokerNodes() {}

  /** Broker {@code id}, its settings overridden by {@code overrides}. */
  public static BrokerNode withOverrides(int id, Map<String, String> overrides) {
    String directory = String.format("broker_%d_data0", id);
    return new BrokerNode(id, Uuid.randomUuid(), directory, List.of(directory), overrides);
  }
}
