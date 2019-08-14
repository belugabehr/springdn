package io.github.belugabehr.datanode.meta.block;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.cloudera.datanode.domain.DataNodeDomain.ChecksumInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

@Service
public class BlockMetaDataService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockMetaDataService.class);

  private static final ReadOptions SKIP_CACHE = new ReadOptions().fillCache(false);

  private static final ReadOptions DEFAULT = new ReadOptions();

  @Value("${dn.meta.dir:/var/lib/springdn}")
  private String dataDir;

  @Value("${dn.meta.cache:33554432}")
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

  public void deletedBlock(final BlockIdentifier blockId) throws IOException {
    Preconditions.checkNotNull(blockId);

    final byte[] blockIdBytes = blockId.toByteArray();

    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);
    final byte[] checksumKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);

    final WriteBatch updates = this.db.createWriteBatch().delete(metaKey).delete(checksumKey);

    this.db.write(updates);
  }

  public void addBlock(final BlockMetaData blockMetaData, final ChecksumInfo checksumInfo) {
    Preconditions.checkNotNull(blockMetaData);
    Preconditions.checkNotNull(checksumInfo);

    LOG.debug("Adding metadata for block: [{}]", blockMetaData);

    final byte[] blockIdBytes = blockMetaData.getBlockId().toByteArray();

    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);
    final byte[] metaValue = blockMetaData.toByteArray();

    final byte[] checksumKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);
    final byte[] checksumValue = checksumInfo.toByteArray();

    final WriteBatch updates = this.db.createWriteBatch().put(metaKey, metaValue).put(checksumKey, checksumValue);
    this.db.write(updates);
  }

  public Optional<BlockMetaData> getBlockMetaData(final BlockIdentifier blockId) throws IOException {
    Preconditions.checkNotNull(blockId);

    final byte[] blockIdBytes = blockId.toByteArray();
    final byte[] metaKey = BlockMetaKeys.BLOCK_META.getKeyValue(blockIdBytes);

    final byte[] result = this.db.get(metaKey);

    return result == null ? Optional.absent() : Optional.of(BlockMetaData.parseFrom(result));
  }

  public BlockMetaIterator getBlockMetaData() throws IOException {
    return new BlockMetaIterator(this.db.iterator(SKIP_CACHE));
  }

  public ChecksumInfo getBlockChecksum(final BlockIdentifier blockId, final boolean skipCache) {
    final byte[] blockIdBytes = blockId.toByteArray();

    final ReadOptions readOptions = (skipCache) ? SKIP_CACHE : DEFAULT;
    final byte[] metaKey = BlockMetaKeys.BLOCK_CHECKSUM.getKeyValue(blockIdBytes);
    final byte[] metaValue = this.db.get(metaKey, readOptions);

    try {
      return ChecksumInfo.parseFrom(metaValue);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}
