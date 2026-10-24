package io.github.bpalermo.kafka.spiffe.workload.h2;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * HPACK (RFC 7541), as much as a client needs: an encoder that writes every field as a literal
 * without indexing and without Huffman coding, which every decoder accepts and which keeps no
 * state, and a full decoder, with the static and dynamic tables, table size updates and
 * Huffman-coded strings, since a server may use all of them.
 *
 * <p>Names and values are octets; they are held as ISO-8859-1 strings, one char per octet.
 */
final class Hpack {

  /** A header field. */
  record Field(String name, String value) {
    /** Its size as RFC 7541 section 4.1 counts it. */
    int size() {
      return name.length() + value.length() + 32;
    }
  }

  /** The static table, RFC 7541 Appendix A; index 1 is element 0. */
  static final List<Field> STATIC_TABLE =
      List.of(
          new Field(":authority", ""),
          new Field(":method", "GET"),
          new Field(":method", "POST"),
          new Field(":path", "/"),
          new Field(":path", "/index.html"),
          new Field(":scheme", "http"),
          new Field(":scheme", "https"),
          new Field(":status", "200"),
          new Field(":status", "204"),
          new Field(":status", "206"),
          new Field(":status", "304"),
          new Field(":status", "400"),
          new Field(":status", "404"),
          new Field(":status", "500"),
          new Field("accept-charset", ""),
          new Field("accept-encoding", "gzip, deflate"),
          new Field("accept-language", ""),
          new Field("accept-ranges", ""),
          new Field("accept", ""),
          new Field("access-control-allow-origin", ""),
          new Field("age", ""),
          new Field("allow", ""),
          new Field("authorization", ""),
          new Field("cache-control", ""),
          new Field("content-disposition", ""),
          new Field("content-encoding", ""),
          new Field("content-language", ""),
          new Field("content-length", ""),
          new Field("content-location", ""),
          new Field("content-range", ""),
          new Field("content-type", ""),
          new Field("cookie", ""),
          new Field("date", ""),
          new Field("etag", ""),
          new Field("expect", ""),
          new Field("expires", ""),
          new Field("from", ""),
          new Field("host", ""),
          new Field("if-match", ""),
          new Field("if-modified-since", ""),
          new Field("if-none-match", ""),
          new Field("if-range", ""),
          new Field("if-unmodified-since", ""),
          new Field("last-modified", ""),
          new Field("link", ""),
          new Field("location", ""),
          new Field("max-forwards", ""),
          new Field("proxy-authenticate", ""),
          new Field("proxy-authorization", ""),
          new Field("range", ""),
          new Field("referer", ""),
          new Field("refresh", ""),
          new Field("retry-after", ""),
          new Field("server", ""),
          new Field("set-cookie", ""),
          new Field("strict-transport-security", ""),
          new Field("transfer-encoding", ""),
          new Field("user-agent", ""),
          new Field("vary", ""),
          new Field("via", ""),
          new Field("www-authenticate", ""));

  private Hpack() {}

  /** Encodes {@code fields} as literals without indexing, with literal names and values. */
  static byte[] encode(List<Field> fields) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Field field : fields) {
      out.write(0x00);
      string(out, field.name());
      string(out, field.value());
    }
    return out.toByteArray();
  }

  private static void string(ByteArrayOutputStream out, String value) {
    byte[] octets = value.getBytes(StandardCharsets.ISO_8859_1);
    integer(out, 0x00, 7, octets.length);
    out.write(octets, 0, octets.length);
  }

  /** Writes an integer with an N-bit prefix (RFC 7541 section 5.1) after the given flag bits. */
  static void integer(ByteArrayOutputStream out, int flags, int prefixBits, int value) {
    int max = (1 << prefixBits) - 1;
    if (value < max) {
      out.write(flags | value);
      return;
    }
    out.write(flags | max);
    int rest = value - max;
    while (rest >= 0x80) {
      out.write((rest & 0x7f) | 0x80);
      rest >>>= 7;
    }
    out.write(rest);
  }

  /**
   * A decoder, with the dynamic table of one connection. It is not thread safe: header blocks are
   * decoded in the order they arrive.
   */
  static final class Decoder {
    private final int maxTableSize;
    private final int maxHeaderListSize;
    private final Deque<Field> table = new ArrayDeque<>();
    private int tableSize;
    private int tableCapacity;

    /**
     * A decoder whose dynamic table may grow to {@code maxTableSize} octets (what the client
     * advertised as SETTINGS_HEADER_TABLE_SIZE) and that refuses header lists larger than {@code
     * maxHeaderListSize}.
     */
    Decoder(int maxTableSize, int maxHeaderListSize) {
      this.maxTableSize = maxTableSize;
      this.maxHeaderListSize = maxHeaderListSize;
      this.tableCapacity = maxTableSize;
    }

    /**
     * Decodes one complete header block.
     *
     * @throws HpackException if it cannot be decoded; the connection must then be closed, since the
     *     table is no longer in step with the encoder's
     */
    List<Field> decode(byte[] block) throws HpackException {
      List<Field> fields = new ArrayList<>();
      int listSize = 0;
      int[] position = {0};
      boolean fieldSeen = false;
      while (position[0] < block.length) {
        int first = block[position[0]] & 0xff;
        Field field;
        if ((first & 0x80) != 0) {
          field = lookup(readInteger(block, position, 7));
        } else if ((first & 0xc0) == 0x40) {
          field = literal(block, position, 6);
          add(field);
        } else if ((first & 0xe0) == 0x20) {
          if (fieldSeen) {
            throw new HpackException("a dynamic table size update after a header field");
          }
          int size = readInteger(block, position, 5);
          if (size > maxTableSize) {
            throw new HpackException(
                "a dynamic table size of " + size + " above the " + maxTableSize + " allowed");
          }
          tableCapacity = size;
          evict(0);
          continue;
        } else {
          // 0000xxxx without indexing, 0001xxxx never indexed: alike to a decoder.
          field = literal(block, position, 4);
        }
        fieldSeen = true;
        listSize += field.size();
        if (listSize > maxHeaderListSize) {
          throw new HpackException("a header list larger than " + maxHeaderListSize + " octets");
        }
        fields.add(field);
      }
      return fields;
    }

    /** The dynamic table's current size, in RFC 7541 octets. */
    int tableSize() {
      return tableSize;
    }

    private Field literal(byte[] block, int[] position, int prefixBits) throws HpackException {
      int index = readInteger(block, position, prefixBits);
      String name = index == 0 ? readString(block, position) : lookup(index).name();
      return new Field(name, readString(block, position));
    }

    private Field lookup(int index) throws HpackException {
      if (index <= 0) {
        throw new HpackException("header table index 0");
      }
      if (index <= STATIC_TABLE.size()) {
        return STATIC_TABLE.get(index - 1);
      }
      int dynamic = index - STATIC_TABLE.size() - 1;
      if (dynamic >= table.size()) {
        throw new HpackException("header table index " + index + " beyond the table");
      }
      int i = 0;
      for (Field field : table) {
        if (i++ == dynamic) {
          return field;
        }
      }
      throw new HpackException("header table index " + index + " beyond the table");
    }

    private void add(Field field) {
      evict(field.size());
      if (field.size() <= tableCapacity) {
        table.addFirst(field);
        tableSize += field.size();
      }
    }

    /** Evicts oldest entries until {@code incoming} more octets fit in the capacity. */
    private void evict(int incoming) {
      while (!table.isEmpty() && tableSize + incoming > tableCapacity) {
        tableSize -= table.removeLast().size();
      }
    }
  }

  /** Reads an integer with an N-bit prefix at {@code position}, advancing it. */
  static int readInteger(byte[] block, int[] position, int prefixBits) throws HpackException {
    int max = (1 << prefixBits) - 1;
    int value = block[position[0]++] & max;
    if (value < max) {
      return value;
    }
    int shift = 0;
    while (true) {
      if (position[0] >= block.length) {
        throw new HpackException("an integer cut short");
      }
      int octet = block[position[0]++] & 0xff;
      if (shift > 21) {
        throw new HpackException("an integer too large");
      }
      value += (octet & 0x7f) << shift;
      if (value < 0) {
        throw new HpackException("an integer too large");
      }
      if ((octet & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
  }

  private static String readString(byte[] block, int[] position) throws HpackException {
    if (position[0] >= block.length) {
      throw new HpackException("a string cut short");
    }
    boolean huffman = (block[position[0]] & 0x80) != 0;
    int length = readInteger(block, position, 7);
    if (length > block.length - position[0]) {
      throw new HpackException("a string longer than its header block");
    }
    byte[] octets =
        huffman
            ? Huffman.decode(block, position[0], length)
            : java.util.Arrays.copyOfRange(block, position[0], position[0] + length);
    position[0] += length;
    return new String(octets, StandardCharsets.ISO_8859_1);
  }
}
