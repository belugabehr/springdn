package io.github.belugabehr.datanode.comms.netty.dt;

import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PacketHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.github.belugabehr.datanode.storage.block.BlockHandle;
import io.github.belugabehr.datanode.storage.block.BlockManager;
import io.github.belugabehr.datanode.util.FixedLengthBlockCheckedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

/**
 * Each packet looks like: PLEN HLEN HEADER CHECKSUMS DATA 32-bit 16-bit
 * <protobuf> <variable length>
 *
 * PLEN: Payload length = length(PLEN) + length(CHECKSUMS) + length(DATA) This
 * length includes its own encoded length in the sum for historical reasons.
 *
 * HLEN: Header length = length(HEADER)
 * 
 * HEADER: the actual packet header fields, encoded in protobuf CHECKSUMS: the
 * crcs for the data chunk. May be missing if checksums were not requested DATA
 * the actual block data
 */
public class PacketInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(PacketInboundHandlerAdapter.class);

  private final FixedLengthBlockCheckedOutputStream checksums;
  private final BlockIdentifier blockID;
  private final BlockHandle blockHandle;
  private final boolean lastInPipeline;

  public PacketInboundHandlerAdapter(final BlockIdentifier blockID, BlockHandle blockHandle, boolean lastInPipeline) {
    this.checksums = new FixedLengthBlockCheckedOutputStream(8192);
    this.blockHandle = blockHandle;
    this.lastInPipeline = lastInPipeline;
    this.blockID = blockID;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    final ByteBuf in = (ByteBuf) msg;
    LOG.debug("Packet size: {}b", in.readableBytes());

    final int packetLength = in.readInt();
    final short headerLength = in.readShort();

    LOG.debug("Payload length: {}b; Header length: {}b", packetLength, headerLength);

    final PacketHeaderProto header;
    try {
      header = PacketHeaderProto.parseFrom(new ByteBufInputStream(in, headerLength));
      LOG.trace("Header: {}::{}", this.blockID.getBlockId(), header);
    } catch (IOException e) {
      LOG.error("Error", e);
      return;
    }

    final int checksumLength = packetLength - Ints.BYTES - header.getDataLen();
    LOG.trace("Checksum [length={}]: {}", checksumLength, this.checksums);

    // TODO: Verify checksums are correct
    in.skipBytes(checksumLength);

    final int dataLength = header.getDataLen();

    final AttributeKey<BlockManager> SM_KEY = AttributeKey.valueOf("storageManager");
    final BlockManager blockManager = ctx.channel().attr(SM_KEY).get();
    Preconditions.checkNotNull(blockManager);

    if (!header.getLastPacketInBlock()) {
      final int skip = Math.toIntExact(this.checksums.getCount() - header.getOffsetInBlock());

      // Sometimes the packets may overlap. However, there should never be gaps
      // between packets.
      Preconditions.checkState(skip >= 0);

      in.skipBytes(skip);
      final ByteBuf data = in.readSlice(dataLength - skip);

      try {
        data.getBytes(data.readerIndex(), this.checksums, data.readableBytes());

        blockManager.appendBlock(this.blockHandle, header.getOffsetInBlock(), data.nioBuffer());
      } catch (IOException e) {
        LOG.error("Error", e);
      }
    } else {
      try {
        this.checksums.close();

        // Number of byte written should line up with the last offset
        Preconditions.checkState(this.checksums.getCount() == header.getOffsetInBlock());

        blockManager.finalizeBlock(this.blockID, this.blockHandle, Math.toIntExact(this.checksums.getCount()),
            this.checksums.getBlockSize(), this.checksums.getChecksums());
      } catch (IOException e) {
        LOG.error("Error", e);
      }
    }

    if (lastInPipeline) {
      final PipelineAckProto ack =
          PipelineAckProto.newBuilder().addReply(Status.SUCCESS).setSeqno(header.getSeqno()).build();
      LOG.debug("ACK {}", ack);
      ctx.writeAndFlush(ack);
    }

    ReferenceCountUtil.release(in);
  }
}
