package io.github.belugabehr.datanode.util;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PacketHeaderProto;
import org.slf4j.Logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

public class ChunkedPacketStream implements ChunkedInput<ByteBuf> {

  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ChunkedPacketStream.class);

  private final BlockPacketWriter packetWriter;
  private int seqNo;
  private boolean eof;

  public ChunkedPacketStream(final BlockPacketWriter packetWriter) {
    this.packetWriter = packetWriter;
    this.seqNo = 0;
    this.eof = false;
  }

  @Override
  public boolean isEndOfInput() throws Exception {
    return this.eof;
  }

  @Override
  public void close() throws Exception {
    this.packetWriter.close();
  }

  @Override
  public ByteBuf readChunk(final ChannelHandlerContext ctx) throws Exception {
    return readChunk(ctx.alloc());
  }

  @Override
  public ByteBuf readChunk(final ByteBufAllocator allocator) throws Exception {
    final ByteBuf packetBuf;

    if (this.seqNo < this.packetWriter.getPacketCount()) {
      final long fileOffset = packetWriter.getFilePosition();
      final int dataLen = packetWriter.getNextPacketPayloadLength();

      final PacketHeaderProto packetHeader = PacketHeaderProto.newBuilder().setLastPacketInBlock(false)
          .setSeqno(this.seqNo).setOffsetInBlock(fileOffset).setDataLen(dataLen).build();
      final ByteBuf packetHeaderBuf = ProtobufEncoder.encode(packetHeader);

      packetBuf = allocator.directBuffer(68 * 1024);
      final int packetStartIndex = packetBuf.writerIndex();

      final int hlen = packetHeaderBuf.readableBytes();
      packetBuf.writeZero(Ints.BYTES + Shorts.BYTES).writeBytes(packetHeaderBuf);

      final int plen = packetWriter.write(packetBuf);

      packetBuf.setInt(packetStartIndex, plen);
      packetBuf.setShort(packetStartIndex + Ints.BYTES, hlen);

      this.seqNo++;
    } else {
      // Write last packet
      packetBuf = allocator.buffer(32);

      final PacketHeaderProto lastPacket = PacketHeaderProto.newBuilder().setLastPacketInBlock(true)
          .setSeqno(this.seqNo).setOffsetInBlock(0).setDataLen(0).build();

      final ByteBuf packetHeaderBuf = ProtobufEncoder.encode(lastPacket);
      final int hlen = packetHeaderBuf.readableBytes();

      packetBuf.writeInt(4).writeShort(hlen).writeBytes(packetHeaderBuf);

      this.eof = true;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Packet Dump: {}", ByteBufUtil.prettyHexDump(packetBuf));
    }
    return packetBuf;
  }

  @Override
  public long length() {
    return -1L;
  }

  @Override
  public long progress() {
    return this.seqNo;
  }

}
