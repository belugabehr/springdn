package io.github.belugabehr.datanode.comms.netty.dt;

import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Shorts;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.stream.ChunkedWriteHandler;

public class OpControlInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(OpControlInboundHandlerAdapter.class);

  private static final ProtobufDecoder OP_READ_DECODER = new ProtobufDecoder(OpReadBlockProto.getDefaultInstance());

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
    final ByteBuf in = (ByteBuf) msg;

    if (!in.isReadable(Shorts.BYTES + 1)) {
      LOG.warn("Closing connection: client did not send enough intial bytes");
      ctx.close();
      return;
    }

    final short version = in.readShort();
    if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
      LOG.warn("Connection made with DataTransferProtocol version {} but expected {}. Closing the connection.", version,
          DataTransferProtocol.DATA_TRANSFER_VERSION);
      ctx.close();
      return;
    }

    final byte opCode = in.readByte();
    final ChannelPipeline p = ctx.pipeline();

    switch (opCode) {
    case 80:
      // Op.WRITE_BLOCK.code
      p.addLast(new ProtobufVarint32LengthFieldPrepender(), new ProtobufEncoder());
      p.addLast(new OpWriteBlockChannelInboundHandler());
      break;
    case 81:
      // Op.READ_BLOCK.code
      p.addLast(new ProtobufVarint32FrameDecoder()).addLast(OP_READ_DECODER);
      p.addLast(new ChunkedWriteHandler());
      p.addLast(new OpReadBlockChannelInboundHandler());
      break;
    default:
      LOG.warn("Unknown OpCode: {}. Closing connection on {}", opCode, ctx.channel().remoteAddress());
      ctx.close();
      return;
    }

    p.remove(this);

    ctx.fireChannelRead(in.slice());
  }
}
