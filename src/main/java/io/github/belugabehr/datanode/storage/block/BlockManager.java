package io.github.belugabehr.datanode.storage.block;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.cloudera.datanode.domain.DataNodeDomain;
import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.cloudera.datanode.domain.DataNodeDomain.ChecksumInfo;
import com.cloudera.datanode.domain.DataNodeDomain.StorageInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import io.github.belugabehr.datanode.events.IncrementalBlockListener;
import io.github.belugabehr.datanode.meta.block.BlockMetaDataService;

@Repository
public class BlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);

  @Autowired
  private BlockStorageService storageService;

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  @Autowired
  private IncrementalBlockListener incrementalBlockReportListener;

  public BlockHandle initializeBlock(final BlockIdentifier blockID, final long blockSize) throws Exception {
    final BlockHandle handle = storageService.initializeBlock(blockID, blockSize);

    final StorageInfo storageInfo = StorageInfo.newBuilder().setBlockSize(Math.toIntExact(blockSize))
        .setVolumeUUID(handle.getVolume().getUuid().toString()).build();

    this.incrementalBlockReportListener.publishBlockReceiving(blockID, storageInfo);
    return handle;
  }

  public void appendBlock(final BlockHandle blockHandle, final long offset, final ByteBuffer data) throws IOException {
    this.storageService.appendBlock(blockHandle, offset, data);
  }

  public void finalizeBlock(final BlockIdentifier blockID, final BlockHandle blockHandle, final int bytesWritten,
      final int checksumChunkSize, final byte[] chunkedChecksums) throws IOException {
    Preconditions.checkArgument(
        ((bytesWritten + checksumChunkSize - 1) / checksumChunkSize) == (chunkedChecksums.length / Ints.BYTES));

    final StorageInfo storageInfo = StorageInfo.newBuilder().setBlockSize(bytesWritten)
        .setVolumeUUID(blockHandle.getVolume().getUuid().toString()).build();

    final ChecksumInfo checksumInfo = ChecksumInfo.newBuilder().setChecksumChunkSize(checksumChunkSize)
        .setChecksumChunks(ByteString.copyFrom(chunkedChecksums)).build();

    final BlockMetaData bmb = DataNodeDomain.BlockMetaData.newBuilder().setBlockId(blockID)
        .setCTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond())).setStorageInfo(storageInfo)
        .setChecksumChunkSize(checksumChunkSize).build();

    // Add to metadata first so if the storage fails, the file will not be
    // orphaned on the volume
    this.blockMetaDataService.addBlock(bmb, checksumInfo);

    this.storageService.finalizeBlock(blockHandle, bytesWritten);

    this.incrementalBlockReportListener.publishBlockReceived(blockID, storageInfo);
  }

  public void handleBlockInvalidate(final Pair<String, Block> pair) throws IOException {
    final Block block = pair.getRight();

    final BlockIdentifier blockID = DataNodeDomain.BlockIdentifier.newBuilder().setBlockPoolId(pair.getLeft())
        .setBlockId(block.getBlockId()).setGenerationStamp(block.getGenerationStamp()).build();

    final Optional<BlockMetaData> blockMeta = this.blockMetaDataService.getBlockMetaData(blockID);

    if (blockMeta.isPresent()) {
      this.storageService.deleteBlock(blockID, blockMeta.get().getStorageInfo().getVolumeUUID());
      this.blockMetaDataService.deletedBlock(blockID);
      this.incrementalBlockReportListener.publishBlockDeleted(blockID, blockMeta.get().getStorageInfo());
    }
  }

  public Pair<BlockMetaData, FileChannel> getBlock(final OpReadBlockProto op) throws IOException {
    final ExtendedBlockProto block = op.getHeader().getBaseHeader().getBlock();

    DataNodeDomain.BlockIdentifier blockID =
        DataNodeDomain.BlockIdentifier.newBuilder().setBlockPoolId(block.getPoolId()).setBlockId(block.getBlockId())
            .setGenerationStamp(block.getGenerationStamp()).build();

    final Optional<BlockMetaData> blockMeta = this.blockMetaDataService.getBlockMetaData(blockID);

    if (!blockMeta.isPresent()) {
      throw new IOException("No such block exists: " + blockID);
    }

    final FileChannel channel = this.storageService.openBlock(blockID, blockMeta.get().getStorageInfo().getVolumeUUID());
    return Pair.of(blockMeta.get(), channel);
  }
  
  public ChecksumInfo getBlockChecksum(final BlockIdentifier blockID) {
    return this.blockMetaDataService.getBlockChecksum(blockID, false);
  }

}
