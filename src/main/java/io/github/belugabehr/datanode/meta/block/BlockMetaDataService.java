package io.github.belugabehr.datanode.meta.block;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockMetaData;
import io.github.belugabehr.datanode.domain.DataNodeDomain.ChecksumInfo;

@Service
public class BlockMetaDataService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockMetaDataService.class);

  private static final ReadOptions SKIP_CACHE = new ReadOptions().fillCache(false);

  private static final ReadOptions DEFAULT = new ReadOptions();

  @Value("${datanode.meta.home:/var/lib/springdn}")
  private String dataDir;

  @Value("${datanode.meta.cache:33554432}")
  private long cacheSize;

  private DB db;

  @PostConstruct
  public void init() throws IOException {
    final Path metaDir = Paths.get(dataDir, "meta");
    Files.createDirectories(metaDir);
    this.db = JniDBFactory.factory.open(metaDir.toFile(), new Options().cacheSize(cacheSize).blockSize(1024 * 32)
        .paranoidChecks(true).compressionType(CompressionType.SNAPPY));
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    LOG.info("Shutting down Block Metadata Store");
    this.db.close();
    LOG.info("Shutting down Block Metadata Store complete");
  }

  public void addBlock(final BlockMetaData blockMetaData, final ChecksumInfo checksumInfo) {
    Objects.requireNonNull(blockMetaData);
    Objects.requireNonNull(checksumInfo);

    LOG.debug("Adding metadata for block: [{}]", blockMetaData);

    final byte[] blockIdBytes = blockMetaData.getBlockId().toByteArray();

    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);
    final byte[] metaValue = blockMetaData.toByteArray();

    final byte[] checksumKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);
    final byte[] checksumValue = checksumInfo.toByteArray();

    final WriteBatch updates = this.db.createWriteBatch().put(metaKey, metaValue).put(checksumKey, checksumValue);
    this.db.write(updates);
  }

  public boolean updateBlock(final BlockMetaData blockMetaData) throws IOException {
    Objects.requireNonNull(blockMetaData);

    final BlockIdentifier blockId = blockMetaData.getBlockId();

    LOG.debug("Updating metadata for block: [{}]", blockId);

    // Block Meta Data and Checksum need to be always linked atomically
    // It may be the case that the block was deleted and then updating the block
    // meta data would create an orphaned meta data record. It would later fail
    // to delete because the delete block operation deletes both.
    final Optional<ChecksumInfo> checksumInfo = getBlockChecksum(blockId, true);
    if (checksumInfo.isPresent()) {
      addBlock(blockMetaData, checksumInfo.get());
      return true;
    }
    return false;
  }

  public boolean deleteBlock(final BlockIdentifier blockId) throws IOException {
    Objects.requireNonNull(blockId);

    LOG.debug("Deleting metadata for block: [{}]", blockId);

    final byte[] blockIdBytes = blockId.toByteArray();

    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);
    final byte[] checksumKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);

    final WriteBatch updates = this.db.createWriteBatch().delete(metaKey).delete(checksumKey);

    try {
      this.db.write(updates);
      return true;
    } catch (final DBException dbe) {
      LOG.debug("Unable to delete block [{}]", blockId, dbe);
      return false;
    }
  }

  public Optional<BlockMetaData> getBlockMetaData(final BlockIdentifier blockId, final boolean skipCache)
      throws IOException {
    Objects.requireNonNull(blockId);

    final byte[] blockIdBytes = blockId.toByteArray();
    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);

    final ReadOptions readOptions = (skipCache) ? SKIP_CACHE : DEFAULT;

    final byte[] result = this.db.get(metaKey, readOptions);

    return result == null ? Optional.empty() : Optional.of(BlockMetaData.parseFrom(result));
  }

  public BlockMetaIterator getBlockMetaData() {
    return new BlockMetaIterator(this.db.iterator(SKIP_CACHE));
  }

  public Optional<ChecksumInfo> getBlockChecksum(final BlockIdentifier blockId, final boolean skipCache)
      throws IOException {
    Objects.requireNonNull(blockId);

    final byte[] blockIdBytes = blockId.toByteArray();
    final byte[] checksumKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);

    final ReadOptions readOptions = (skipCache) ? SKIP_CACHE : DEFAULT;

    final byte[] result = this.db.get(checksumKey, readOptions);
    return result == null ? Optional.empty() : Optional.of(ChecksumInfo.parseFrom(result));
  }

}
