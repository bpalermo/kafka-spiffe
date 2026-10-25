package io.github.bpalermo.kafka.spiffe.jar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The published artifact (design, section 9): one jar of the library's own classes, Java 17
 * bytecode, a manifest naming the module, a POM with only the provided dependencies and what Maven
 * Central requires, and sources and javadoc for every class.
 */
class JarTest {

  private static final String PACKAGE = "io/github/bpalermo/kafka/spiffe/";

  /** The class file version of Java 17. */
  private static final int JAVA_17 = 61;

  private static Path file(String property) {
    return Path.of(System.getProperty(property));
  }

  private static List<String> entries(Path jar) throws IOException {
    try (JarFile file = new JarFile(jar.toFile())) {
      return Collections.list(file.entries()).stream()
          .filter(e -> !e.isDirectory())
          .map(ZipEntry::getName)
          .toList();
    }
  }

  /** The top-level classes of the jar, as source paths. */
  private static Set<String> topLevelSources(List<String> entries) {
    Set<String> sources = new TreeSet<>();
    for (String entry : entries) {
      if (entry.endsWith(".class") && !entry.contains("$")) {
        sources.add(entry.substring(0, entry.length() - ".class".length()) + ".java");
      }
    }
    return sources;
  }

  @Test
  void theJarHoldsOnlyTheLibrarysClasses() throws IOException {
    List<String> others = new ArrayList<>();
    for (String entry : entries(file("jar.binary"))) {
      if (!entry.startsWith(PACKAGE) && !"META-INF/MANIFEST.MF".equals(entry)) {
        others.add(entry);
      }
    }
    assertEquals(List.of(), others);
    List<String> entries = entries(file("jar.binary"));
    assertTrue(entries.contains(PACKAGE + "kafka/SpiffeSslEngineFactory.class"));
    assertTrue(entries.contains(PACKAGE + "kafka/SpiffePrincipalBuilder.class"));
  }

  @Test
  void everyClassIsJava17Bytecode() throws IOException {
    List<String> wrong = new ArrayList<>();
    try (JarFile jar = new JarFile(file("jar.binary").toFile())) {
      for (ZipEntry entry : Collections.list(jar.entries())) {
        if (!entry.getName().endsWith(".class")) {
          continue;
        }
        try (InputStream in = jar.getInputStream(entry);
            DataInputStream data = new DataInputStream(in)) {
          assertEquals(0xCAFEBABE, data.readInt(), entry.getName());
          data.readUnsignedShort();
          int major = data.readUnsignedShort();
          if (major != JAVA_17) {
            wrong.add(entry.getName() + ": " + major);
          }
        }
      }
    }
    assertEquals(List.of(), wrong);
  }

  @Test
  void theManifestNamesTheModuleAndTheVersion() throws Exception {
    Manifest manifest;
    try (JarFile jar = new JarFile(file("jar.binary").toFile())) {
      manifest = jar.getManifest();
    }
    assertNotNull(manifest);
    Attributes main = manifest.getMainAttributes();
    assertEquals("io.github.bpalermo.kafka.spiffe", main.getValue("Automatic-Module-Name"));
    assertEquals("kafka-spiffe", main.getValue("Implementation-Title"));
    assertEquals(text(pom(), "version"), main.getValue("Implementation-Version"));
  }

  @Test
  void thePomHasOnlyTheProvidedDependencies() throws Exception {
    Element project = pom();
    assertEquals("io.github.bpalermo", text(project, "groupId"));
    assertEquals("kafka-spiffe", text(project, "artifactId"));
    List<String> dependencies = new ArrayList<>();
    NodeList nodes = project.getElementsByTagName("dependency");
    for (int i = 0; i < nodes.getLength(); i++) {
      Element dependency = (Element) nodes.item(i);
      dependencies.add(
          String.join(
              ":",
              text(dependency, "groupId"),
              text(dependency, "artifactId"),
              text(dependency, "version"),
              text(dependency, "scope")));
    }
    assertEquals(
        List.of(
            "org.apache.kafka:kafka-clients:3.0.0:provided", "org.slf4j:slf4j-api:1.7.36:provided"),
        dependencies);
  }

  /** What Maven Central refuses a POM without. */
  @Test
  void thePomHasWhatMavenCentralRequires() throws Exception {
    Element project = pom();
    for (String element : List.of("name", "description", "url")) {
      assertFalse(text(project, element).isBlank(), element);
    }
    Element license = child(child(project, "licenses"), "license");
    assertEquals("Apache License, Version 2.0", text(license, "name"));
    assertFalse(text(child(child(project, "developers"), "developer"), "name").isBlank());
    Element scm = child(project, "scm");
    for (String element : List.of("connection", "developerConnection", "url")) {
      assertFalse(text(scm, element).isBlank(), "scm " + element);
    }
  }

  @Test
  void everyClassHasItsSource() throws IOException {
    Set<String> missing = topLevelSources(entries(file("jar.binary")));
    missing.removeAll(entries(file("jar.sources")));
    assertEquals(Set.of(), missing);
  }

  @Test
  void theJavadocDocumentsThePublicEntryPoints() throws IOException {
    List<String> docs = entries(file("jar.docs"));
    assertTrue(docs.contains("index.html"), "index.html");
    assertTrue(docs.contains(PACKAGE + "kafka/SpiffeSslEngineFactory.html"));
    assertTrue(docs.contains(PACKAGE + "kafka/SpiffePrincipalBuilder.html"));
  }

  private static Element pom() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    Document document = factory.newDocumentBuilder().parse(file("jar.pom").toFile());
    return document.getDocumentElement();
  }

  /** The only direct child of {@code parent} named {@code name}. */
  private static Element child(Element parent, String name) {
    Element found = null;
    for (var node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && element.getTagName().equals(name)) {
        assertEquals(null, found, "more than one " + name);
        found = element;
      }
    }
    assertNotNull(found, name);
    return found;
  }

  private static String text(Element parent, String name) {
    return child(parent, name).getTextContent().trim();
  }
}
