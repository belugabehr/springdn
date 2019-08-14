package io.github.belugabehr.datanode.storage;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.cloudera.datanode.domain.DataNodeDomain.ChecksumInfo;
import com.google.common.io.LimitInputStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.github.belugabehr.datanode.meta.block.BlockMetaDataService;
import io.github.belugabehr.datanode.meta.block.BlockMetaIterator;
import io.github.belugabehr.datanode.util.FixedLengthBlockCheckedOutputStream;
import io.github.belugabehr.datanode.util.ex.BlockNotFoundException;
import io.github.belugabehr.datanode.util.ex.ChecksumException;

@Component
public class BlockStorageScanner implements Runnable, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStorageScanner.class);

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  @Autowired
  private BlockStorageService storageService;

  private volatile Thread t;

  public void init() {
    this.t = new ThreadFactoryBuilder().setNameFormat("Block Checker").setDaemon(true).build().newThread(this);
    t.start();
  }

  @Override
  public void run() {
    while (true) {
      // Race condition when closing the service - LevelDB is closed while this
      // is still accessing
      try (final BlockMetaIterator blockMetas = this.blockMetaDataService.getBlockMetaData()) {
        while (blockMetas.hasNext()) {
          final BlockMetaData blockMeta = blockMetas.next();
          try {
            LOG.debug("Validating checksum for {}", blockMeta.getBlockId());
            final ChecksumInfo checksumInfo = this.blockMetaDataService.getBlockChecksum(blockMeta.getBlockId(), true);
            validateChecksum(blockMeta, checksumInfo);
          } catch (ChecksumException cse) {
            try {
              this.storageService.deleteBlock(blockMeta.getBlockId(), blockMeta.getStorageInfo().getVolumeUUID());
            } catch (IOException ioe) {
              LOG.warn("Unable to delete dile with corrupt checksum: {}", blockMeta.getBlockId());
            } finally {
              this.blockMetaDataService.deletedBlock(blockMeta.getBlockId());
            }
          } catch (FileNotFoundException | BlockNotFoundException nfe) {
            LOG.error("Block is missing", nfe);
            this.blockMetaDataService.deletedBlock(blockMeta.getBlockId());
          } catch (Exception e) {
            LOG.warn("Could not check block: {}", blockMeta.getBlockId());
          }
        }
        Thread.sleep(8000L);
      } catch (InterruptedException ie) {
        LOG.info("BlockChecker was interrupted. Exiting");
        return;
      } catch (Exception e) {
        LOG.warn("BlockChecker expereinced an error. Will retry.", e);
      }
    }
  }

  private void validateChecksum(final BlockMetaData bm, final ChecksumInfo checksumInfo) throws Exception {
    final BlockIdentifier blockID = bm.getBlockId();

    final FixedLengthBlockCheckedOutputStream os = new FixedLengthBlockCheckedOutputStream(bm.getChecksumChunkSize());

    try (final FileChannel channel = this.storageService.openBlock(blockID, bm.getStorageInfo().getVolumeUUID())) {

      final InputStream is = new LimitInputStream(Channels.newInputStream(channel), bm.getStorageInfo().getBlockSize());

      is.transferTo(os);

      is.close();
      os.close();
    } catch (IOException ioe) {
      throw ioe;
    }

    if (!Arrays.equals(checksumInfo.getChecksumChunks().toByteArray(), os.getChecksums())) {
      throw new ChecksumException();
    }
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    this.t.interrupt();
  }

}
