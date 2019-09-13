package io.github.belugabehr.datanode.events;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import io.github.belugabehr.datanode.storage.block.BlockManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;

@Component
public class InvalidateBlockListener {
  private static final Logger LOG = LoggerFactory.getLogger(InvalidateBlockListener.class);
  private final Flux<Pair<String, Block>> publishedFlux;
  private final FluxSink<Pair<String, Block>> sink;

  @Autowired
  private BlockManager blockManager;

  @Autowired
  private ThreadPoolTaskExecutor taskExecutor;

  public InvalidateBlockListener() {
    final UnicastProcessor<Pair<String, Block>> unicastProcessor = UnicastProcessor.create(new LinkedBlockingQueue<>());
    this.publishedFlux = unicastProcessor.publish().autoConnect(0);
    this.sink = unicastProcessor.sink();
  }

  @PostConstruct
  public void init() {
    this.publishedFlux.publishOn(Schedulers.fromExecutor(taskExecutor)).subscribe(p -> {
      try {
        blockManager.handleBlockInvalidate(p);
      } catch (Exception ioe) {
        LOG.error("Error", ioe);
      }
    });
  }

  public void publish(final BlockCommand command) {
    try {
      Arrays.asList(command.getBlocks()).forEach(block -> {
        LOG.debug("Delete requested [{}] {}", command.getBlockPoolId(), block);
        this.sink.next(Pair.of(command.getBlockPoolId(), block));
      });
    } catch (RuntimeException rte) {
      LOG.error("Error", rte);
    }
  }

}
