package io.github.bpalermo.kafka.spiffe.workload;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * The part of the protobuf wire format the Workload API's X.509 responses need: varints,
 * length-delimited fields, and skipping whatever else a newer agent sends. No generated code and no
 * protobuf runtime (design, section 3).
 */
final class WireReader {

  static final int VARINT = 0;
  static final int FIXED64 = 1;
  static final int LENGTH_DELIMITED = 2;
  static final int FIXED32 = 5;

  private final byte[] bytes;
  private final int end;
  private int position;

  WireReader(byte[] bytes) {
    this(bytes, 0, bytes.length);
  }

  private WireReader(byte[] bytes, int from, int to) {
    this.bytes = bytes;
    this.position = from;
    this.end = to;
  }

  /** Whether a field remains. */
  boolean hasNext() {
    return position < end;
  }

  /**
   * Reads the next field's tag: its number in the high bits, its wire type in the low three.
   *
   * @throws WireException if the input ends inside the tag, or the field number is 0
   */
  int tag() throws WireException {
    long tag = varint();
    if (tag >>> 3 == 0 || tag > Integer.MAX_VALUE) {
      throw new WireException("an invalid field tag " + tag);
    }
    return (int) tag;
  }

  /** Reads a length-delimited field's octets. */
  byte[] bytes() throws WireException {
    int length = length();
    byte[] out = Arrays.copyOfRange(bytes, position, position + length);
    position += length;
    return out;
  }

  /** Reads a length-delimited field as UTF-8. */
  String string() throws WireException {
    return new String(bytes(), StandardCharsets.UTF_8);
  }

  /** A reader over a length-delimited field, an embedded message. */
  WireReader message() throws WireException {
    int length = length();
    WireReader reader = new WireReader(bytes, position, position + length);
    position += length;
    return reader;
  }

  /** Skips a field of the given wire type. */
  void skip(int wireType) throws WireException {
    switch (wireType) {
      case VARINT -> varint();
      case FIXED64 -> advance(8);
      case LENGTH_DELIMITED -> advance(length());
      case FIXED32 -> advance(4);
      default -> throw new WireException("an unsupported wire type " + wireType);
    }
  }

  private int length() throws WireException {
    long length = varint();
    if (length > end - position) {
      throw new WireException("a field of " + length + " octets beyond the message");
    }
    return (int) length;
  }

  private void advance(int count) throws WireException {
    if (count > end - position) {
      throw new WireException("a field beyond the message");
    }
    position += count;
  }

  private long varint() throws WireException {
    long value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      if (position >= end) {
        throw new WireException("the message ends inside a varint");
      }
      byte b = bytes[position++];
      value |= (long) (b & 0x7f) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
    }
    throw new WireException("a varint longer than ten octets");
  }

  /** A message that is not valid protobuf. */
  static final class WireException extends Exception {
    private static final long serialVersionUID = 1L;

    WireException(String message) {
      super(message);
    }
  }
}
