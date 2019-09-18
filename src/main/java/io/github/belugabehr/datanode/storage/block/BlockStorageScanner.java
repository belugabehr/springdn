package io.github.belugabehr.datanode.storage.block;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockMetaData;
import io.github.belugabehr.datanode.domain.DataNodeDomain.ChecksumInfo;
import io.github.belugabehr.datanode.domain.DataNodeDomain.StorageInfo;
import io.github.belugabehr.datanode.meta.block.BlockMetaDataService;
import io.github.belugabehr.datanode.meta.block.BlockMetaIterator;
import io.github.belugabehr.datanode.util.FixedLengthBlockCheckedOutputStream;
import io.github.belugabehr.datanode.util.ex.ChecksumException;

@Component
public class BlockStorageScanner implements Runnable, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStorageScanner.class);

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  @Autowired
  private BlockStorageService blockStorageService;

  private volatile Thread t;

  @PostConstruct
  public void init() {
    // TODO: Perhaps add this to one of the global thread pool
    this.t = new ThreadFactoryBuilder().setNameFormat("Block Checker").setDaemon(true).build().newThread(this);
    t.start();
  }

  @Override
  public void run() {
    while (true) {
      // TODO: Race condition when closing the service - LevelDB is closed while
      // this is still accessing
      try (final BlockMetaIterator blockMetaIter = this.blockMetaDataService.getBlockMetaData()) {
        while (blockMetaIter.hasNext()) {
          final BlockMetaData blockMeta = blockMetaIter.next();
          LOG.debug("Checking block [{}]", blockMeta.getBlockId());

          final Optional<ChecksumInfo> checksumInfo =
              this.blockMetaDataService.getBlockChecksum(blockMeta.getBlockId(), true);
          if (checksumInfo.isEmpty()) {
            // Block was just deleted - skip it
            continue;
          }

          try {
            final String volumeId = validateChecksum(blockMeta, checksumInfo.get());

            final List<String> volumeIds = blockMeta.getStorageInfo().getVolumeIdList();
            if (volumeIds.size() > 1 && Iterables.getLast(volumeIds).equals(volumeId)) {
              removeBlockCopies(blockMeta, Iterables.getLast(volumeIds));
            }
          } catch (ChecksumException cse) {
            LOG.warn("Deleting block [{}]", blockMeta.getBlockId());
            // TODO: Need to delete at the top-level, not just the meta
            this.blockMetaDataService.deleteBlock(blockMeta.getBlockId());
            this.blockStorageService.deleteBlock(blockMeta.getBlockId(), blockMeta.getStorageInfo().getVolumeIdList());
          } catch (Exception e) {
            LOG.warn("Could not check block: {}", blockMeta.getBlockId(), e);
          }
        }
        // TODO: This is arbitrary
        Thread.sleep(8000L);
      } catch (InterruptedException ie) {
        LOG.info("BlockChecker was interrupted. Exiting");
        return;
      } catch (Exception e) {
        LOG.warn("BlockChecker expereinced an error. Will retry.", e);
      }
    }
  }

  private void removeBlockCopies(final BlockMetaData blockMeta, final String volumeId) throws IOException {
    final StorageInfo.Builder updatedStorageInfo =
        StorageInfo.newBuilder(blockMeta.getStorageInfo()).clearVolumeId().addVolumeId(volumeId);

    final BlockMetaData updateBlockMetaData =
        BlockMetaData.newBuilder(blockMeta).setStorageInfo(updatedStorageInfo).build();

    this.blockMetaDataService.updateBlock(updateBlockMetaData);
  }

  private String validateChecksum(final BlockMetaData bm, final ChecksumInfo checksumInfo) throws Exception {
    final BlockIdentifier blockID = bm.getBlockId();

    LOG.debug("Validating checksum for block [{}]", blockID);

    final List<String> volumeIds = Lists.newArrayList(bm.getStorageInfo().getVolumeIdList());

    // Start with last volume first
    for (final String volumeId : Lists.reverse(volumeIds)) {
      final FixedLengthBlockCheckedOutputStream os = new FixedLengthBlockCheckedOutputStream(bm.getChecksumChunkSize());

      try (final FileChannel channel = this.blockStorageService.openBlock(blockID, volumeId)) {
        final InputStream is = ByteStreams.limit(Channels.newInputStream(channel), bm.getStorageInfo().getBlockSize());
        is.transferTo(os);
      } catch (final IOException ioe) {
        LOG.warn("Could not determine checksum for block [{}][volumeId:{}]", blockID, volumeId, ioe);
      } finally {
        Closeables.close(os, true);
      }

      if (!Arrays.equals(checksumInfo.getChecksumChunks().toByteArray(), os.getChecksums())) {
        LOG.warn("Invalid checksum for block [{}][volumeId:{}]", blockID, volumeId);
        continue;
      }

      return volumeId;
    }
    throw new ChecksumException();
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    this.t.interrupt();
  }

}
