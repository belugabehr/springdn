package io.github.belugabehr.datanode.comms.netty.dt;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;

@Configuration
public class DataTransferConfig {

  @Bean(name = "dtBossGroup")
  public NioEventLoopGroup dtBossGroup() {
    return new NioEventLoopGroup();
  }

  @Bean(name = "dtWorkerGroup")
  public NioEventLoopGroup dtWorkerGroup() {
    return new NioEventLoopGroup();
  }

  @Bean(name = "dtServerBootstrap")
  public ServerBootstrap dtServerBootstrap() {
    return new ServerBootstrap().option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
  }

  @Bean(name = "dtServerConnectionCounter")
  public ConnectionCounterChannelHandler dtConnectionCounterChannelHandler() {
    return new ConnectionCounterChannelHandler();
  }

}
