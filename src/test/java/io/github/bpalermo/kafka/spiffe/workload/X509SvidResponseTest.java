package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.CodedOutputStream;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.testing.FakeWorkloadApi;
import io.github.bpalermo.kafka.spiffe.testing.TestPki;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Responses encoded by protobuf's own CodedOutputStream, decoded by the hand-written reader. */
class X509SvidResponseTest {

  private static final TestPki.Authority OWN = TestPki.root("example.org");
  private static final TestPki.Authority FEDERATED = TestPki.root("k8s.example.org");

  @Test
  void decodesSvidsBundlesAndFederatedBundles() throws Exception {
    TestPki.Issued broker = OWN.intermediate().svid("/broker");
    TestPki.Issued other = OWN.svid("/other");
    byte[] encoded =
        new FakeWorkloadApi.Response()
            .svid(FakeWorkloadApi.Svid.of(broker, OWN, "internal"))
            .svid(FakeWorkloadApi.Svid.of(other, OWN))
            .federated("spiffe://k8s.example.org", List.of(FEDERATED.certificate()))
            .withUnknownField()
            .encode();
    X509SvidResponse response = X509SvidResponse.parse(encoded);
    assertEquals(2, response.svids().size());
    assertEquals("spiffe://example.org/broker", response.svids().get(0).id().toString());
    assertEquals("internal", response.svids().get(0).hint());
    assertEquals(broker.chain(), response.svids().get(0).chain());
    assertEquals(broker.key(), response.svids().get(0).key());
    assertEquals(
        Set.of(TrustDomain.parse("example.org"), TrustDomain.parse("k8s.example.org")),
        response.bundles().keySet());
    assertEquals(
        List.of(FEDERATED.certificate()),
        response.bundles().get(TrustDomain.parse("k8s.example.org")).authorities());
  }

  @Test
  void acceptsAFederatedBundleKeyedByName() throws Exception {
    byte[] encoded =
        new FakeWorkloadApi.Response()
            .svid(FakeWorkloadApi.Svid.of(OWN.svid("/a"), OWN))
            .federated("k8s.example.org", List.of(FEDERATED.certificate()))
            .encode();
    assertEquals(2, X509SvidResponse.parse(encoded).bundles().size());
  }

  @Test
  void aFederatedBundleNeverReplacesTheOneDeliveredWithAnSvid() throws Exception {
    TestPki.Authority impostor = TestPki.root("example.org");
    byte[] encoded =
        new FakeWorkloadApi.Response()
            .svid(FakeWorkloadApi.Svid.of(OWN.svid("/a"), OWN))
            .federated("spiffe://example.org", List.of(impostor.certificate()))
            .encode();
    assertEquals(
        List.of(OWN.certificate()),
        X509SvidResponse.parse(encoded)
            .bundles()
            .get(TrustDomain.parse("example.org"))
            .authorities());
  }

  @Test
  void decodesAnRsaKey() throws Exception {
    TestPki.Issued rsa = OWN.leaf().uris("spiffe://example.org/r").rsa().issue();
    byte[] encoded =
        new FakeWorkloadApi.Response().svid(FakeWorkloadApi.Svid.of(rsa, OWN)).encode();
    assertEquals("RSA", X509SvidResponse.parse(encoded).svids().get(0).key().getAlgorithm());
  }

  @Test
  void decodesAResponseWithNoSvid() throws Exception {
    assertEquals(0, X509SvidResponse.parse(new byte[0]).svids().size());
  }

  @Test
  void refusesMalformedResponses() throws Exception {
    TestPki.Issued issued = OWN.svid("/a");
    List<byte[]> malformed =
        List.of(
            new byte[] {0x0a, 0x05, 0x01}, // an SVID longer than the message
            new byte[] {0x0a}, // a tag and nothing else
            new byte[] {(byte) 0x80}, // a varint cut short
            new byte[] {0x00}, // field number 0
            new byte[] {0x0b}, // wire type 3, a group
            svid(
                "spiffe://example.org/a",
                issued.leaf().getEncoded(),
                new byte[] {1, 2},
                issued.leaf().getEncoded()),
            svid(
                "spiffe://example.org/a",
                new byte[] {1, 2},
                issued.key().getEncoded(),
                issued.leaf().getEncoded()),
            svid(
                "spiffe://example.org/b",
                issued.leaf().getEncoded(),
                issued.key().getEncoded(),
                issued.leaf().getEncoded()),
            svid(
                "spiffe://Example.org/a",
                issued.leaf().getEncoded(),
                issued.key().getEncoded(),
                issued.leaf().getEncoded()),
            svid(
                "spiffe://example.org/a",
                issued.leaf().getEncoded(),
                issued.key().getEncoded(),
                new byte[0]),
            svidWithout(issued));
    for (int i = 0; i < malformed.size(); i++) {
      byte[] message = malformed.get(i);
      assertThrows(
          X509SvidResponse.InvalidResponseException.class,
          () -> X509SvidResponse.parse(message),
          "case " + i);
    }
  }

  private static byte[] svid(String id, byte[] chain, byte[] key, byte[] bundle)
      throws IOException {
    ByteArrayOutputStream inner = new ByteArrayOutputStream();
    CodedOutputStream message = CodedOutputStream.newInstance(inner);
    message.writeString(1, id);
    message.writeByteArray(2, chain);
    message.writeByteArray(3, key);
    message.writeByteArray(4, bundle);
    message.flush();
    ByteArrayOutputStream outer = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(outer);
    out.writeByteArray(1, inner.toByteArray());
    out.flush();
    return outer.toByteArray();
  }

  /** An SVID without its bundle field. */
  private static byte[] svidWithout(TestPki.Issued issued) throws Exception {
    ByteArrayOutputStream inner = new ByteArrayOutputStream();
    CodedOutputStream message = CodedOutputStream.newInstance(inner);
    message.writeString(1, "spiffe://example.org/a");
    message.writeByteArray(2, issued.leaf().getEncoded());
    message.writeByteArray(3, issued.key().getEncoded());
    message.flush();
    ByteArrayOutputStream outer = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(outer);
    out.writeByteArray(1, inner.toByteArray());
    out.flush();
    return outer.toByteArray();
  }
}
