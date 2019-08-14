package io.github.belugabehr.datanode.events;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.cloudera.datanode.domain.DataNodeDomain.StorageInfo;

import io.github.belugabehr.datanode.blockpool.BlockPoolManager;
import io.github.belugabehr.datanode.comms.nn.NameNodeConnectionPool;
import io.github.belugabehr.datanode.util.Batch;
import io.github.belugabehr.datanode.util.HadoopCompatible;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
public class IncrementalBlockListener {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementalBlockListener.class);

  @Autowired
  private BlockPoolManager blockPoolManager;

  @Autowired
  private NameNodeConnectionPool connections;

  @Autowired
  private ThreadPoolTaskExecutor taskExecutor;

  private final Flux<Tuple2<URI, IncrementalBlockBatch>> publishedFlux;
  private final FluxSink<Wrapper> sink;

  public IncrementalBlockListener() {
    final UnicastProcessor<Wrapper> unicastProcessor = UnicastProcessor.create(new LinkedBlockingQueue<>());
    this.publishedFlux = unicastProcessor
        .groupBy(wrapper -> wrapper.getBlockID().getBlockPoolId()).flatMap(group -> group.take(Duration.ofMillis(50L))
            .collect(() -> new IncrementalBlockBatch(group.key()), (batch, bmd) -> batch.add(bmd)))
        .flatMap(i -> getNameNodes(i)).publish().autoConnect(1);
    this.sink = unicastProcessor.sink();
  }

  @PostConstruct
  public void init() {
    this.publishedFlux.publishOn(Schedulers.fromExecutor(taskExecutor)).subscribe(ibrs -> {
      this.report(ibrs);
    });
  }

  private void report(final Tuple2<URI, IncrementalBlockBatch> ibrs) {
    final Collection<Wrapper> batch = ibrs.getT2().getItems();

    final DatanodeRegistration registration = this.blockPoolManager.getDatanodeRegistration(ibrs.getT2().getKey());

    Flux.fromIterable(batch).collectMultimap(bmd -> bmd.getStorageInfo().getVolumeUUID()).subscribe(x -> {
      final Map<String, Collection<Wrapper>> volumeMap = x;

      LOG.debug("Reporting incrementally blocks: {}", volumeMap);

      final List<StorageReceivedDeletedBlocks> send = new ArrayList<>();

      for (final String volumeUUID : volumeMap.keySet()) {
        final DatanodeStorage storage = new DatanodeStorage(HadoopCompatible.getDatanodeStorageUuid(volumeUUID));

        final Collection<ReceivedDeletedBlockInfo> blocks = volumeMap.get(volumeUUID).stream()
            .collect(Collectors.toMap(Wrapper::getBlockID, Function.identity(), (s, a) -> a)).values().stream()
            .map(wrapper -> new ReceivedDeletedBlockInfo(new Block(wrapper.getBlockID().getBlockId(),
                wrapper.getStorageInfo().getBlockSize(), wrapper.getBlockID().getGenerationStamp()),
                wrapper.getBlockStatus(), null))
            .collect(Collectors.toList());

        send.add(new StorageReceivedDeletedBlocks(storage, blocks.toArray(new ReceivedDeletedBlockInfo[0])));
      }

      DatanodeProtocolClientSideTranslatorPB connection = null;
      try {
        connection = this.connections.borrowObject(ibrs.getT1());

        LOG.info("Sending incremental block report: {}", send);

        connection.blockReceivedAndDeleted(registration, registration.getNamespaceInfo().getBlockPoolID(),
            send.toArray(new StorageReceivedDeletedBlocks[0]));

      } catch (final Exception e) {
        LOG.error("Failed to update NameNode with block receiving notification", e);
      } finally {
        this.connections.returnObject(ibrs.getT1(), connection);
      }
    });
  }

  public void publishBlockDeleted(final BlockIdentifier blockID, StorageInfo storageInfo) {
    LOG.debug("Deleted block incremental report [{}]", blockID);
    this.sink.next(new Wrapper(blockID, storageInfo, BlockStatus.DELETED_BLOCK));
  }

  public void publishBlockReceiving(BlockIdentifier blockID, StorageInfo storageInfo) {
    LOG.debug("Receiving block incremental report [{}] {}", blockID, storageInfo);
    this.sink.next(new Wrapper(blockID, storageInfo, BlockStatus.RECEIVING_BLOCK));
  }

  public void publishBlockReceived(BlockIdentifier blockID, StorageInfo storageInfo) {
    LOG.debug("Received block incremental report [{}] {}", blockID, storageInfo);
    this.sink.next(new Wrapper(blockID, storageInfo, BlockStatus.RECEIVED_BLOCK));
  }

  private Flux<Tuple2<URI, IncrementalBlockBatch>> getNameNodes(final IncrementalBlockBatch batch) {
    final Collection<URI> nameNodes = blockPoolManager.getNameNodes(batch.getKey());
    return Flux.fromIterable(nameNodes).map(nn -> Tuples.of(nn, batch));
  }

  private class Wrapper {
    private final BlockIdentifier blockID;
    private final StorageInfo storageInfo;
    private final BlockStatus blockStatus;

    public Wrapper(BlockIdentifier blockID, StorageInfo storageInfo, BlockStatus blockStatus) {
      this.blockID = blockID;
      this.storageInfo = storageInfo;
      this.blockStatus = blockStatus;
    }

    public BlockIdentifier getBlockID() {
      return blockID;
    }

    public StorageInfo getStorageInfo() {
      return storageInfo;
    }

    public BlockStatus getBlockStatus() {
      return blockStatus;
    }

    @Override
    public String toString() {
      return "Wrapper [blockID=" + blockID + ", storageInfo=" + storageInfo + ", blockStatus=" + blockStatus + "]";
    }
  }

  private class IncrementalBlockBatch extends Batch<String, Wrapper> {
    public IncrementalBlockBatch(String key) {
      super(key);
    }
  }

  public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
    Set<Object> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());
    return t -> seen.add(keyExtractor.apply(t));
  }
}
