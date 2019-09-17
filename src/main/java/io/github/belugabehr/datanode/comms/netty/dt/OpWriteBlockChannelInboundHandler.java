package io.github.belugabehr.datanode.comms.netty.dt;

import java.util.Optional;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.datanode.domain.DataNodeDomain;
import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.storage.block.BlockHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;

public class OpWriteBlockChannelInboundHandler extends DefaultSimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(OpWriteBlockChannelInboundHandler.class);

  private static final BlockOpResponseProto OK_RESPONSE =
      BlockOpResponseProto.newBuilder().setStatus(Status.SUCCESS).build();

  private ByteBuf opWriteBuf;

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    this.opWriteBuf = ctx.alloc().buffer(256);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    this.opWriteBuf.release();
    this.opWriteBuf = null;
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf buf) throws Exception {
    this.opWriteBuf.writeBytes(buf);

    final Optional<ByteBuf> opWriteProtoBuf = decodeOpWriteBlock(this.opWriteBuf);

    if (opWriteProtoBuf.isEmpty()) {
      return;
    }

    final ByteBuf msg = opWriteProtoBuf.get();
    final byte[] array;
    final int offset;
    final int length = msg.readableBytes();
    if (msg.hasArray()) {
      array = msg.array();
      offset = msg.arrayOffset() + msg.readerIndex();
    } else {
      array = ByteBufUtil.getBytes(msg, msg.readerIndex(), length, false);
      offset = 0;
    }

    final OpWriteBlockProto op =
        OpWriteBlockProto.getDefaultInstance().getParserForType().parseFrom(array, offset, length);

    final ExtendedBlockProto block = op.getHeader().getBaseHeader().getBlock();
    final boolean lastInPipeline = op.getTargetsCount() == 0;

    final BlockIdentifier blockID = DataNodeDomain.BlockIdentifier.newBuilder().setBlockPoolId(block.getPoolId())
        .setBlockId(block.getBlockId()).setGenerationStamp(block.getGenerationStamp()).build();

    final BlockHandle blockHandle =
        getStorageManager(ctx).initializeBlock(op.getStorageId(), blockID, block.getNumBytes());

    if (!lastInPipeline) {
      ctx.pipeline().addLast(new OpWriteBlockProxyFrontendHandler(op));
    }

    ctx.pipeline().addLast(new HdfsPacketFrameDecoder());

    ctx.pipeline().addLast(new PacketInboundHandlerAdapter(blockID, blockHandle, lastInPipeline));

    if (lastInPipeline) {
      ctx.writeAndFlush(OK_RESPONSE);
    }

    ByteBuf tmp = this.opWriteBuf.retainedSlice();
    LOG.debug("Readble bytes: {}", tmp.readableBytes());
    ctx.fireChannelRead(tmp);

    ctx.pipeline().remove(this);
  }

  private Optional<ByteBuf> decodeOpWriteBlock(final ByteBuf in) throws Exception {
    in.markReaderIndex();
    int preIndex = in.readerIndex();
    int length = readRawVarint32(in);
    if (preIndex == in.readerIndex()) {
      return Optional.empty();
    }
    if (length < 0) {
      throw new CorruptedFrameException("negative length: " + length);
    }

    if (in.readableBytes() < length) {
      in.resetReaderIndex();
      return Optional.empty();
    }
    return Optional.of(in.readSlice(length));
  }

  /**
   * Reads variable length 32bit int from buffer
   *
   * @return decoded int if buffers readerIndex has been forwarded else nonsense
   *         value
   */
  private static int readRawVarint32(ByteBuf buffer) {
    if (!buffer.isReadable()) {
      return 0;
    }
    buffer.markReaderIndex();
    byte tmp = buffer.readByte();
    if (tmp >= 0) {
      return tmp;
    } else {
      int result = tmp & 127;
      if (!buffer.isReadable()) {
        buffer.resetReaderIndex();
        return 0;
      }
      if ((tmp = buffer.readByte()) >= 0) {
        result |= tmp << 7;
      } else {
        result |= (tmp & 127) << 7;
        if (!buffer.isReadable()) {
          buffer.resetReaderIndex();
          return 0;
        }
        if ((tmp = buffer.readByte()) >= 0) {
          result |= tmp << 14;
        } else {
          result |= (tmp & 127) << 14;
          if (!buffer.isReadable()) {
            buffer.resetReaderIndex();
            return 0;
          }
          if ((tmp = buffer.readByte()) >= 0) {
            result |= tmp << 21;
          } else {
            result |= (tmp & 127) << 21;
            if (!buffer.isReadable()) {
              buffer.resetReaderIndex();
              return 0;
            }
            result |= (tmp = buffer.readByte()) << 28;
            if (tmp < 0) {
              throw new CorruptedFrameException("malformed varint.");
            }
          }
        }
      }
      return result;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    ctx.close();
    LOG.error("Error", cause);
  }
}
