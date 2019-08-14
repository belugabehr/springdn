package io.github.belugabehr.datanode.comms.netty.dt;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@Sharable
public class ConnectionCounterChannelHandler extends ChannelInboundHandlerAdapter {

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    this.connectionCount.incrementAndGet();
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    this.connectionCount.decrementAndGet();
  }

  public AtomicInteger getConnectionCount() {
    return this.connectionCount;
  }
}
