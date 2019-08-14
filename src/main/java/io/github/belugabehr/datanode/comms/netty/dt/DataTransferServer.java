package io.github.belugabehr.datanode.comms.netty.dt;

import java.io.Closeable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.github.belugabehr.datanode.has.Metrics;
import io.github.belugabehr.datanode.storage.BlockManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;

@Component
public class DataTransferServer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DataTransferServer.class);

  @Autowired
  private BlockManager storageManager;

  @Autowired
  private MeterRegistry meterRegistry;

  @Value("${dn.ipc.transfer.port:51515}")
  private int dataTransferPort;

  @Autowired
  @Qualifier("dtServerBootstrap")
  private ServerBootstrap serverBootstrap;

  @Autowired
  @Qualifier("dtBossGroup")
  private NioEventLoopGroup bossGroup;

  @Autowired
  @Qualifier("dtWorkerGroup")
  private NioEventLoopGroup workerGroup;

  @Autowired
  private ConnectionCounterChannelHandler connectionCounterChannelHandler;

  private ChannelFuture serverChannel;

  @PostConstruct
  public void init() throws InterruptedException {
    this.serverBootstrap.group(this.bossGroup, this.workerGroup).channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            // Incoming
            ch.pipeline().addFirst(connectionCounterChannelHandler).addLast(new ReadTimeoutHandler(120))
                .addLast(new OpControlInboundHandlerAdapter());
          }
        }).childAttr(AttributeKey.valueOf("storageManager"), this.storageManager);

    this.meterRegistry.gauge(Metrics.IPC_XCEIVER_TOTAL_COUNT.registryName(), workerGroup, wg -> wg.executorCount());

    this.meterRegistry.gauge(Metrics.IPC_XCEIVER_ACTIVE_COUNT.registryName(),
        this.connectionCounterChannelHandler.getConnectionCount());

    this.serverChannel = this.serverBootstrap.bind(dataTransferPort).sync();
  }

  @PreDestroy
  public void close() {
    LOG.info("Shutting down Data Transfer Server");
    try {
      // Wait until the server socket is closed
      // this.serverChannel.channel().closeFuture().sync();
      Thread.sleep(15L);

      this.workerGroup.shutdownGracefully();
      this.bossGroup.shutdownGracefully();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Data Transfer Server shutdown interrupted");
    } finally {

    }
    LOG.info("Shutting down Data Transfer Server complete");
  }

}
