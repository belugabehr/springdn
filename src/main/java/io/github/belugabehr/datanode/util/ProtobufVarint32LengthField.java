package io.github.belugabehr.datanode.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public final class ProtobufVarint32LengthField {

  private ProtobufVarint32LengthField() {

  }

  public static ByteBuf getLength(final ByteBuf msg) throws Exception {
    int bodyLen = msg.readableBytes();
    int headerLen = computeRawVarint32Size(bodyLen);
    final ByteBuf out = Unpooled.buffer(headerLen);
    writeRawVarint32(out, bodyLen);
    return out;
  }

  /**
   * Writes protobuf varint32 to (@link ByteBuf).
   * 
   * @param out to be written to
   * @param value to be written
   */
  static void writeRawVarint32(final ByteBuf out, int value) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        out.writeByte(value);
        return;
      } else {
        out.writeByte((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  /**
   * Computes size of protobuf varint32 after encoding.
   * 
   * @param value which is to be encoded.
   * @return size of value encoded as protobuf varint32.
   */
  static int computeRawVarint32Size(final int value) {
    if ((value & (0xffffffff << 7)) == 0) {
      return 1;
    }
    if ((value & (0xffffffff << 14)) == 0) {
      return 2;
    }
    if ((value & (0xffffffff << 21)) == 0) {
      return 3;
    }
    if ((value & (0xffffffff << 28)) == 0) {
      return 4;
    }
    return 5;
  }
}
