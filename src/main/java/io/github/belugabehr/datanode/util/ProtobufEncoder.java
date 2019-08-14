package io.github.belugabehr.datanode.util;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

public final class ProtobufEncoder {
  private ProtobufEncoder() {

  }

  public static ByteBuf encode(final MessageLite msg) throws Exception {
    return wrappedBuffer(msg.toByteArray());
  }

  public static ByteBuf encode(MessageLite.Builder msg) throws Exception {
    return encode(msg.build());
  }
}
