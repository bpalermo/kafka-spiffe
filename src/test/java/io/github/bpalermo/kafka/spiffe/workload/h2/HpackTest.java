package io.github.bpalermo.kafka.spiffe.workload.h2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bpalermo.kafka.spiffe.workload.h2.Hpack.Field;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** RFC 7541's examples, its error cases, and Netty's encoder and decoder as a second opinion. */
class HpackTest {

  private static byte[] hex(String spaced) {
    return HexFormat.of().parseHex(spaced.replace(" ", ""));
  }

  private static Hpack.Decoder decoder() {
    return new Hpack.Decoder(4096, 64 << 10);
  }

  private static final List<Field> FIRST =
      List.of(
          new Field(":method", "GET"),
          new Field(":scheme", "http"),
          new Field(":path", "/"),
          new Field(":authority", "www.example.com"));
  private static final List<Field> SECOND =
      List.of(
          new Field(":method", "GET"),
          new Field(":scheme", "http"),
          new Field(":path", "/"),
          new Field(":authority", "www.example.com"),
          new Field("cache-control", "no-cache"));
  private static final List<Field> THIRD =
      List.of(
          new Field(":method", "GET"),
          new Field(":scheme", "https"),
          new Field(":path", "/index.html"),
          new Field(":authority", "www.example.com"),
          new Field("custom-key", "custom-value"));

  @Test
  void decodesTheRequestsOfAppendixC3() throws HpackException {
    Hpack.Decoder decoder = decoder();
    assertEquals(FIRST, decoder.decode(hex("8286 8441 0f77 7777 2e65 7861 6d70 6c65 2e63 6f6d")));
    assertEquals(57, decoder.tableSize());
    assertEquals(SECOND, decoder.decode(hex("8286 84be 5808 6e6f 2d63 6163 6865")));
    assertEquals(110, decoder.tableSize());
    assertEquals(
        THIRD,
        decoder.decode(
            hex("8287 85bf 400a 6375 7374 6f6d 2d6b 6579 0c63 7573 746f 6d2d 7661 6c75 65")));
    assertEquals(164, decoder.tableSize());
  }

  @Test
  void decodesTheHuffmanCodedRequestsOfAppendixC4() throws HpackException {
    Hpack.Decoder decoder = decoder();
    assertEquals(FIRST, decoder.decode(hex("8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff")));
    assertEquals(57, decoder.tableSize());
    assertEquals(SECOND, decoder.decode(hex("8286 84be 5886 a8eb 1064 9cbf")));
    assertEquals(110, decoder.tableSize());
    assertEquals(
        THIRD, decoder.decode(hex("8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf")));
    assertEquals(164, decoder.tableSize());
  }

  @Test
  void codesIntegersAsAppendixC1Does() throws HpackException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Hpack.integer(out, 0, 5, 10);
    Hpack.integer(out, 0, 5, 1337);
    Hpack.integer(out, 0, 8, 42);
    assertArrayEquals(hex("0a 1f9a0a 2a"), out.toByteArray());
    int[] position = {0};
    byte[] bytes = out.toByteArray();
    assertEquals(10, Hpack.readInteger(bytes, position, 5));
    assertEquals(1337, Hpack.readInteger(bytes, position, 5));
    assertEquals(42, Hpack.readInteger(bytes, position, 8));
  }

  @Test
  void refusesWhatCannotBeDecoded() {
    for (String block :
        List.of(
            "80", // index 0
            "be", // index 62, with an empty dynamic table
            "ff 00", // index 127, beyond both tables
            "3f e2 1f", // a dynamic table size of 4097, above the 4096 allowed
            "82 20", // a dynamic table size update after a field
            "00 05 61", // a name cut short
            "00 01 61 85 aa", // a Huffman value cut short
            "ff ff ff ff ff ff", // an integer that does not end
            "00 01 61 81 00")) { // Huffman padding that is not all ones
      assertThrows(HpackException.class, () -> decoder().decode(hex(block)), block);
    }
  }

  @Test
  void refusesAHeaderListTooLarge() {
    Hpack.Decoder small = new Hpack.Decoder(4096, 100);
    byte[] block = Hpack.encode(List.of(new Field("a", "b".repeat(80))));
    assertThrows(HpackException.class, () -> small.decode(block));
  }

  @Test
  void evictsAndShrinksTheDynamicTable() throws Exception {
    DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();
    Hpack.Decoder decoder = decoder();
    for (int round = 0; round < 50; round++) {
      Http2Headers headers = new DefaultHttp2Headers();
      headers.add("x-key-" + (round % 7), "value-" + round + "-" + "v".repeat(round * 3));
      assertEquals(fields(headers), decoder.decode(encode(encoder, headers)));
      if (round == 20) {
        encoder.configuration().maxHeaderTableSize(256);
      }
      if (round == 30) {
        encoder.configuration().maxHeaderTableSize(0);
      }
      if (round == 40) {
        encoder.configuration().maxHeaderTableSize(4096);
      }
    }
  }

  @Test
  void decodesWhatNettysEncoderWritesOfEveryOctet() throws Exception {
    DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();
    Hpack.Decoder decoder = decoder();
    Random random = new Random(42);
    for (int round = 0; round < 200; round++) {
      Http2Headers headers = new DefaultHttp2Headers(false);
      for (int i = 0; i < 1 + random.nextInt(6); i++) {
        byte[] value = new byte[random.nextInt(40)];
        for (int j = 0; j < value.length; j++) {
          value[j] = (byte) random.nextInt(256);
        }
        headers.add("h" + random.nextInt(20), new AsciiString(value));
      }
      assertEquals(fields(headers), decoder.decode(encode(encoder, headers)), "round " + round);
    }
  }

  @Test
  void nettyDecodesWhatTheEncoderWrites() throws Exception {
    List<Field> fields =
        List.of(
            new Field(":method", "POST"),
            new Field(":path", "/SpiffeWorkloadAPI/FetchX509SVID"),
            new Field("workload.spiffe.io", "true"),
            new Field("long", "x".repeat(300)));
    Http2Headers decoded =
        new DefaultHttp2HeadersDecoder(false)
            .decodeHeaders(1, Unpooled.wrappedBuffer(Hpack.encode(fields)));
    List<Field> back = new ArrayList<>();
    for (Map.Entry<CharSequence, CharSequence> entry : decoded) {
      back.add(new Field(entry.getKey().toString(), entry.getValue().toString()));
    }
    assertEquals(fields, back);
  }

  @Test
  void theStaticTableHasItsSixtyOneEntries() {
    assertEquals(61, Hpack.STATIC_TABLE.size());
    assertEquals(new Field(":authority", ""), Hpack.STATIC_TABLE.get(0));
    assertEquals(new Field("www-authenticate", ""), Hpack.STATIC_TABLE.get(60));
  }

  private static byte[] encode(DefaultHttp2HeadersEncoder encoder, Http2Headers headers)
      throws Exception {
    ByteBuf buffer = Unpooled.buffer();
    encoder.encodeHeaders(1, headers, buffer);
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes);
    return bytes;
  }

  private static List<Field> fields(Http2Headers headers) {
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<CharSequence, CharSequence> entry : headers) {
      fields.add(
          new Field(
              new String(AsciiString.of(entry.getKey()).toByteArray(), StandardCharsets.ISO_8859_1),
              new String(
                  AsciiString.of(entry.getValue()).toByteArray(), StandardCharsets.ISO_8859_1)));
    }
    return fields;
  }
}
