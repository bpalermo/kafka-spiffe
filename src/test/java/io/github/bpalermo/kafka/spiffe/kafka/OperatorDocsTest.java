package io.github.bpalermo.kafka.spiffe.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

/**
 * docs/operator.md says what the code does: its configuration reference lists every {@code
 * spiffe.*} key with the {@code ConfigDef}'s type and default, and every properties example is a
 * configuration the library accepts, read as Kafka reads a properties file.
 */
class OperatorDocsTest {

  private static final Pattern ROW =
      Pattern.compile("^\\| `(spiffe\\.[^`]+)` \\| ([^|]+) \\| ([^|]+) \\|");
  private static final Pattern PROPERTIES =
      Pattern.compile("^```properties\\n(.*?)^```$", Pattern.MULTILINE | Pattern.DOTALL);

  private static final Set<String> LISTENER_KEYS =
      Set.of(
          "spiffe.principal.listener.<listener>.allowed.ids",
          "spiffe.principal.listener.<listener>.allowed.trust.domains",
          "spiffe.principal.listener.<listener>.allowed.id.patterns");

  private static String document() throws IOException {
    return Files.readString(Path.of(System.getProperty("docs.operator")), StandardCharsets.UTF_8);
  }

  /** The rows of the configuration reference: key -> {type, default}. */
  private static Map<String, String[]> reference() throws IOException {
    String document = document();
    int start = document.indexOf("\n## Configuration reference");
    assertTrue(start >= 0, "no configuration reference");
    Map<String, String[]> rows = new TreeMap<>();
    for (String line : document.substring(start).split("\n")) {
      Matcher row = ROW.matcher(line);
      if (row.find()) {
        String[] previous =
            rows.put(row.group(1), new String[] {row.group(2).trim(), row.group(3).trim()});
        assertEquals(null, previous, "listed twice: " + row.group(1));
      }
    }
    return rows;
  }

  @Test
  void theReferenceListsEveryKeyAndNoOther() throws IOException {
    Set<String> expected = new TreeSet<>(SpiffeConfig.DEFINITION.names());
    expected.addAll(LISTENER_KEYS);
    assertEquals(expected, reference().keySet());
  }

  @Test
  void theReferenceGivesTheTypesAndDefaultsOfTheConfigDef() throws IOException {
    Map<String, String[]> reference = reference();
    Map<String, Object> defaults = SpiffeConfig.DEFINITION.defaultValues();
    for (ConfigDef.ConfigKey key : SpiffeConfig.DEFINITION.configKeys().values()) {
      String[] row = reference.get(key.name);
      assertNotNull(row, key.name + " is not in the reference");
      String type = key.type.name().toLowerCase(Locale.ROOT);
      if (key.validator instanceof ConfigDef.ValidString) {
        assertTrue(row[0].startsWith("`"), key.name + ": the values, not the type: " + row[0]);
      } else {
        assertEquals(type, row[0], key.name);
      }
      Object value = defaults.get(key.name);
      boolean literal =
          value != null && !"".equals(value) && !(value instanceof List<?> list && list.isEmpty());
      if (literal) {
        assertEquals("`" + value + "`", row[1], key.name);
      } else {
        assertTrue(!row[1].isEmpty(), key.name + " says what happens when it is unset");
      }
    }
  }

  @Test
  void everyPropertiesExampleIsAConfigurationTheLibraryAccepts() throws IOException {
    Matcher block = PROPERTIES.matcher(document());
    List<String> checked = new ArrayList<>();
    while (block.find()) {
      Properties properties = new Properties();
      // As Kafka reads a properties file (Utils.loadProps): backslashes are escapes.
      properties.load(new StringReader(block.group(1)));
      Map<String, Object> configs = new HashMap<>();
      properties.forEach((k, v) -> configs.put((String) k, v));
      // A fragment is checked on a minimal configuration it does not replace.
      configs.putIfAbsent("spiffe.endpoint.socket", "unix:///run/spire/agent.sock");
      if (!configs.containsKey("spiffe.peer.allowed.ids")
          && !configs.containsKey("spiffe.peer.allowed.trust.domains")) {
        configs.put("spiffe.peer.allowed.trust.domains", "example.org,k8s.example.org");
      }
      SpiffeConfig config = SpiffeConfig.parse(configs, Map.of());
      PrincipalPolicy policy = PrincipalPolicy.of(config, configs);
      if (configs.containsKey("spiffe.principal.mapping.rules")) {
        PrincipalPolicy.Outcome outcome =
            policy.principal(SpiffeId.parse("spiffe://k8s.example.org/ns/payments/sa/api"), null);
        assertNotNull(outcome.principal());
        assertEquals(
            "Spiffe:spiffe-group:k8s.example.org:payments",
            outcome.principal().toString(),
            "the mapping rule example, read from a properties file: " + outcome.refusal());
      }
      checked.add(block.group(1).lines().findFirst().orElse(""));
    }
    assertTrue(checked.size() >= 5, "too few properties examples found: " + checked);
  }
}
