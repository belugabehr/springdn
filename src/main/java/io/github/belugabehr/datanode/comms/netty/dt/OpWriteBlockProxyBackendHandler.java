package io.github.belugabehr.datanode.comms.netty.dt;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.PipelineAckProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.protobuf.ProtobufDecoder;

public class OpWriteBlockProxyBackendHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(OpWriteBlockProxyBackendHandler.class);

  private static final ProtobufDecoder BLOCK_OP_RESPONSE_DECODER =
      new ProtobufDecoder(BlockOpResponseProto.getDefaultInstance());

  private static final ProtobufDecoder PIPELINE_ACK_DECODER =
      new ProtobufDecoder(PipelineAckProto.getDefaultInstance());

  private enum DataTransferState {
    BLOCK_OP_RESPONSE, PIPELINE_ACK_INIT, PACKET_ACK_RESPONSE
  }

  private final Channel frontendChannel;

  private final BlockOpResponseHandler blockOpResponseHandler;

  private final PipelineAckHandler pipelineAckHandler;

  private DataTransferState state;

  public OpWriteBlockProxyBackendHandler(final Channel inboundChannel) {
    this.frontendChannel = inboundChannel;
    this.state = DataTransferState.BLOCK_OP_RESPONSE;
    this.blockOpResponseHandler = new BlockOpResponseHandler(frontendChannel);
    this.pipelineAckHandler = new PipelineAckHandler(frontendChannel);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    switch (this.state) {
    case BLOCK_OP_RESPONSE:
      ctx.pipeline().addLast(BLOCK_OP_RESPONSE_DECODER);
      ctx.pipeline().addLast(this.blockOpResponseHandler);
      ctx.fireChannelRead(msg);
      this.state = DataTransferState.PIPELINE_ACK_INIT;
      break;
    case PIPELINE_ACK_INIT:
      ctx.pipeline().remove(BLOCK_OP_RESPONSE_DECODER);
      ctx.pipeline().remove(this.blockOpResponseHandler);
      ctx.pipeline().addLast(PIPELINE_ACK_DECODER);
      ctx.pipeline().addLast(this.pipelineAckHandler);
      this.state = DataTransferState.PACKET_ACK_RESPONSE;
      /* Fall through */
    case PACKET_ACK_RESPONSE:
      ctx.fireChannelRead(msg);
      break;
    default:
      throw new Exception();
    }

  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    LOG.warn("Channel Inactive");
    if (frontendChannel.isActive()) {
      frontendChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    LOG.error("Error", cause);
    if (frontendChannel.isActive()) {
      frontendChannel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }
  }

  private static class BlockOpResponseHandler extends DefaultSimpleChannelInboundHandler<BlockOpResponseProto> {

    private final Channel frontendChannel;

    public BlockOpResponseHandler(Channel frontendChannel) {
      this.frontendChannel = frontendChannel;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BlockOpResponseProto msg) throws Exception {
      LOG.debug("Got BlockOpResponseProto: {}", msg);
      frontendChannel.writeAndFlush(msg);
    }

  }

  private static class PipelineAckHandler extends DefaultSimpleChannelInboundHandler<PipelineAckProto> {

    private final Channel frontendChannel;

    public PipelineAckHandler(Channel frontendChannel) {
      this.frontendChannel = frontendChannel;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PipelineAckProto msg) throws Exception {
      LOG.debug("Got PipelineAckProto: {}", msg);
      frontendChannel.writeAndFlush(msg);
    }

  }

}
