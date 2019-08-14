package io.github.belugabehr.datanode.comms.netty.dt;

import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DatanodeInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class OpWriteBlockProxyFrontendHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(OpWriteBlockProxyFrontendHandler.class);

  private final OpWriteBlockProto writeProto;

  private volatile Channel outboundChannel;

  public OpWriteBlockProxyFrontendHandler(final OpWriteBlockProto proxyWriteBlockProto) {
    this.writeProto = proxyWriteBlockProto;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    final Channel inboundChannel = ctx.channel();

    final DatanodeInfoProto dataNodeInfo = this.writeProto.getTargets(0);

    // Start the connection attempt.
    Bootstrap b = new Bootstrap();
    b.group(inboundChannel.eventLoop()).channel(ctx.channel().getClass())
        .handler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new ProtobufVarint32FrameDecoder())
                .addLast(new OpWriteBlockProxyBackendHandler(inboundChannel));

            ch.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender()).addLast(new ProtobufEncoder());
          }

        });

    ChannelFuture f = b.connect(dataNodeInfo.getId().getHostName(), dataNodeInfo.getId().getXferPort());
    outboundChannel = f.channel();
    f.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (future.isSuccess()) {
          outboundChannel.pipeline().firstContext()
              .write(Unpooled.buffer(2).writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION));

          // 80 = Write Opt
          outboundChannel.pipeline().firstContext().write(Unpooled.buffer(1).writeByte((byte) 80));

          final OpWriteBlockProto newWriteProto = getNextHopOp(writeProto);

          outboundChannel.writeAndFlush(newWriteProto);

          LOG.debug("Connected to next dataNode downstream: {}", dataNodeInfo);

        } else {
          // Close the connection if the connection attempt has failed.
          LOG.error("Failed");
          inboundChannel.close();
        }
      }
    });
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, Object msg) {
    final ByteBuf in = (ByteBuf) msg;
    final boolean channelActive = outboundChannel.isActive();
    LOG.debug("Proxy writing {}b to the next DataNode. Active: {}", in.readableBytes(), channelActive);
    if (outboundChannel.isActive()) {
      outboundChannel.pipeline().firstContext().writeAndFlush(in.retainedSlice())
          .addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
              if (!future.isSuccess()) {
                LOG.warn("Operation not succesful. Closing connection with downstream DataNode");
                future.channel().close();
              }
            }
          });
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (outboundChannel != null) {
      closeOnFlush(outboundChannel);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Caught Exception", cause);
    closeOnFlush(ctx.channel());
  }

  private OpWriteBlockProto getNextHopOp(final OpWriteBlockProto op) {
    final OpWriteBlockProto.Builder builder = OpWriteBlockProto.newBuilder(op);

    builder.clearTargets().addAllTargets(op.getTargetsList().subList(1, op.getTargetsCount()));

    if (builder.getTargetStorageTypesCount() > 0) {
      builder.clearTargetStorageTypes()
          .addAllTargetStorageTypes(op.getTargetStorageTypesList().subList(1, op.getTargetStorageTypesCount()));
    }

    if (builder.getTargetPinningsCount() > 0) {
      builder.clearTargetPinnings()
          .addAllTargetPinnings(op.getTargetPinningsList().subList(1, op.getTargetPinningsCount()));
    }
    
    if (builder.getTargetStorageIdsCount() > 0) {
      builder.clearTargetStorageIds()
          .addAllTargetStorageIds(op.getTargetStorageIdsList().subList(1, op.getTargetStorageIdsCount()));
    }

    return builder.build();
  }

  /**
   * Closes the specified channel after all queued write requests are flushed.
   */
  static void closeOnFlush(Channel ch) {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }
}
