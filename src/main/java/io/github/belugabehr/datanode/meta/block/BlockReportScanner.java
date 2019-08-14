package io.github.belugabehr.datanode.meta.block;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.BlockReportReplica;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs.Builder;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.cloudera.datanode.domain.DataNodeDomain.StorageInfo;

import io.github.belugabehr.datanode.blockpool.BlockPoolManager;
import io.github.belugabehr.datanode.comms.nn.NameNodeConnectionPool;
import io.github.belugabehr.datanode.util.Batch;
import io.github.belugabehr.datanode.util.HadoopCompatible;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
public class BlockReportScanner {

  private static Logger LOG = LoggerFactory.getLogger(BlockReportScanner.class);

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  @Autowired
  private BlockPoolManager blockPoolManager;

  @Autowired
  private NameNodeConnectionPool connections;

  @Autowired
  private ThreadPoolTaskExecutor taskExecutor;

  @Autowired
  @Qualifier("globalScheduledTaskExecutor")
  private ScheduledExecutorService scheduledExecutorService;

  @PostConstruct
  public void schedule() {
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          scan(TimeUnit.MINUTES, 0L);
        } catch (Exception e) {
          LOG.warn("Error", e);
        }
      }
    }, 1, 1, TimeUnit.HOURS);
  }

  public void scan(final TimeUnit timeUnit, final long duration) throws Exception {
    final Instant now = Instant.now();
    final long blockReportId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

    LOG.info("Starting a scan of the block meta data for a block report");

    try (final BlockMetaIterator blockMetas = this.blockMetaDataService.getBlockMetaData()) {
      Iterable<BlockMetaData> iterable = () -> (blockMetas);
      Flux.fromIterable(iterable).filter(bmd -> {
        final Instant cTime = Instant.ofEpochSecond(bmd.getCTime().getSeconds());
        return (Duration.between(cTime, now).toMillis() >= timeUnit.toMillis(duration));
      }).groupBy(v -> v.getBlockId().getBlockPoolId())
          // group.take(16).collect() - to do in small batches (not allowed in
          // current protocol)
          // ...
          // Pair BlockID and StorageInfo to allow large checksum info to be
          // GC'ed
          .flatMap(group -> group.collect(() -> new BlockBatch(group.key()),
              (batch, bmd) -> batch.add(Pair.of(bmd.getBlockId(), bmd.getStorageInfo()))))
          .flatMap(i -> getNameNodes(i)).publishOn(Schedulers.fromExecutor(taskExecutor))
          .subscribe(brg -> sendBlockReport(blockReportId, brg));
    }

  }

  private Flux<Tuple2<URI, BlockBatch>> getNameNodes(final BlockBatch batch) {
    final Collection<URI> nameNodes = blockPoolManager.getNameNodes(batch.getKey());
    return Flux.fromIterable(nameNodes).map(nn -> Tuples.of(nn, batch));
  }

  private void sendBlockReport(final long blockReportID, final Tuple2<URI, BlockBatch> brg) {
    final Collection<Pair<BlockIdentifier, StorageInfo>> batch = brg.getT2().getItems();
    Flux.fromIterable(batch).collectMultimap(p -> p.getValue().getVolumeUUID()).subscribe(pmap -> {
      final Map<String, Collection<Pair<BlockIdentifier, StorageInfo>>> volumeMap = pmap;

      LOG.debug("Reporting blocks: {}", volumeMap);

      final List<StorageBlockReport> storageBlockReports = new ArrayList<>();
      for (final String volumeUUID : volumeMap.keySet()) {
        final Builder blockListBuilder = BlockListAsLongs.builder();
        volumeMap.get(volumeUUID).stream()
            .map(p -> new Block(p.getKey().getBlockId(), p.getValue().getBlockSize(), p.getKey().getGenerationStamp()))
            .map(block -> new BlockReportReplica(block)).sorted().collect(Collectors.toList())
            .forEach(brr -> blockListBuilder.add(brr));

        storageBlockReports.add(new StorageBlockReport(
            new DatanodeStorage(HadoopCompatible.getDatanodeStorageUuid(volumeUUID)), blockListBuilder.build()));
      }

      DatanodeProtocolClientSideTranslatorPB connection = null;
      final DatanodeRegistration registration = this.blockPoolManager.getDatanodeRegistration(brg.getT2().getKey());
      try {
        connection = this.connections.borrowObject(brg.getT1());

        final Iterator<StorageBlockReport> iter = storageBlockReports.iterator();
        for (int i = 0; iter.hasNext(); i++) {
          final StorageBlockReport storageBlockReport = iter.next();
          final StorageBlockReport[] report = new StorageBlockReport[] { storageBlockReport };
          connection.blockReport(registration, registration.getNamespaceInfo().getBlockPoolID(), report,
              new BlockReportContext(storageBlockReports.size(), i, blockReportID, 0L, true));
        }
      } catch (Exception e) {
        LOG.warn("Error", e);
      } finally {
        this.connections.returnObject(brg.getT1(), connection);
      }
    });
  }

  class BlockBatch extends Batch<String, Pair<BlockIdentifier, StorageInfo>> {
    public BlockBatch(final String blockPoolID) {
      super(blockPoolID);
    }
  }
}
