package io.github.belugabehr.datanode.storage.block;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ExtendedBlockProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import io.github.belugabehr.datanode.domain.DataNodeDomain;
import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockMetaData;
import io.github.belugabehr.datanode.domain.DataNodeDomain.ChecksumInfo;
import io.github.belugabehr.datanode.domain.DataNodeDomain.StorageInfo;
import io.github.belugabehr.datanode.events.IncrementalBlockListener;
import io.github.belugabehr.datanode.meta.block.BlockMetaDataService;
import io.github.belugabehr.datanode.meta.block.BlockMetaIterator;
import io.github.belugabehr.datanode.storage.Volume;

@Repository
public class BlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);

  @Autowired
  private BlockStorageService blockStorageService;

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  @Autowired
  private IncrementalBlockListener incrementalBlockReportListener;

  public BlockHandle initializeBlock(final String storageId, final BlockIdentifier blockID, final long blockSize)
      throws Exception {

    // Strip "DS-" no idea why it is setup like that
    final UUID volumeGroupId = UUID.fromString(storageId.substring(3));
    final BlockHandle blockHandle = blockStorageService.initializeBlock(volumeGroupId, blockID, blockSize);

    // TODO: This is used and thrown away... somehow pass this to the
    // finalizeBlock method
    final StorageInfo storageInfo = StorageInfo.newBuilder().setVolumeGroupId(volumeGroupId.toString())
        .setBlockSize(Math.toIntExact(blockSize)).addVolumeId(blockHandle.getVolume().getId().toString()).build();

    this.incrementalBlockReportListener.publishBlockReceiving(blockID, storageInfo);
    return blockHandle;
  }

  public void appendBlock(final BlockHandle blockHandle, final long offset, final ByteBuffer data) throws IOException {
    this.blockStorageService.appendBlock(blockHandle, offset, data);
  }

  public void finalizeBlock(final BlockIdentifier blockId, final BlockHandle blockHandle, final long bytesWritten,
      final int checksumChunkSize, final byte[] chunkedChecksums) throws IOException {
    Preconditions.checkArgument(
        ((bytesWritten + checksumChunkSize - 1) / checksumChunkSize) == (chunkedChecksums.length / Ints.BYTES));

    final StorageInfo storageInfo = StorageInfo.newBuilder().setVolumeGroupId(blockHandle.getVolumeGroupId().toString())
        .setBlockSize(Math.toIntExact(bytesWritten)).addVolumeId(blockHandle.getVolume().getId().toString()).build();

    final ChecksumInfo checksumInfo = ChecksumInfo.newBuilder().setChecksumChunkSize(checksumChunkSize)
        .setChecksumChunks(ByteString.copyFrom(chunkedChecksums)).build();

    final BlockMetaData bmb = DataNodeDomain.BlockMetaData.newBuilder().setBlockId(blockId)
        .setCTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond())).setStorageInfo(storageInfo)
        .setChecksumChunkSize(checksumChunkSize).build();

    // Add to metadata first so if the storage fails, the file will not be
    // orphaned on the volume
    this.blockMetaDataService.addBlock(bmb, checksumInfo);

    this.blockStorageService.finalizeBlock(blockHandle, bytesWritten);

    this.incrementalBlockReportListener.publishBlockReceived(blockId, storageInfo);
  }

  public void handleBlockInvalidate(final Pair<String, Block> pair) throws IOException {
    final Block block = pair.getRight();

    final BlockIdentifier blockID = DataNodeDomain.BlockIdentifier.newBuilder().setBlockPoolId(pair.getLeft())
        .setBlockId(block.getBlockId()).setGenerationStamp(block.getGenerationStamp()).build();

    final Optional<BlockMetaData> blockMeta = this.blockMetaDataService.getBlockMetaData(blockID, true);

    if (blockMeta.isPresent()) {
      this.blockStorageService.deleteBlock(blockID, blockMeta.get().getStorageInfo().getVolumeIdList());
      this.blockMetaDataService.deleteBlock(blockID);
      this.incrementalBlockReportListener.publishBlockDeleted(blockID, blockMeta.get().getStorageInfo());
    }
  }

  public Pair<BlockMetaData, FileChannel> getBlock(final OpReadBlockProto op) throws IOException {
    final ExtendedBlockProto block = op.getHeader().getBaseHeader().getBlock();

    final DataNodeDomain.BlockIdentifier blockID =
        DataNodeDomain.BlockIdentifier.newBuilder().setBlockPoolId(block.getPoolId()).setBlockId(block.getBlockId())
            .setGenerationStamp(block.getGenerationStamp()).build();

    final Optional<BlockMetaData> blockMeta = this.blockMetaDataService.getBlockMetaData(blockID, false);

    if (!blockMeta.isPresent()) {
      throw new IOException("No such block exists: " + blockID);
    }

    final Collection<String> volumeIds = blockMeta.get().getStorageInfo().getVolumeIdList();
    for (final String volumeId : volumeIds) {
      try {
        final FileChannel channel = this.blockStorageService.openBlock(blockID, volumeId);
        return Pair.of(blockMeta.get(), channel);
      } catch (IOException ioe) {
        LOG.warn("Could not open block [{}][{}]", blockID, volumeId);
      }
    }
    throw new IOException("Could not get block");
  }

  public Optional<ChecksumInfo> getBlockChecksum(final BlockIdentifier blockID) throws IOException {
    return this.blockMetaDataService.getBlockChecksum(blockID, false);
  }

  public void relocateAnyBlock(final Volume srcVolume, final Volume dstVolume, final TimeUnit unit,
      final long duration) {
    final String srcVolumeId = srcVolume.getId().toString();

    try (final BlockMetaIterator iter = blockMetaDataService.getBlockMetaData()) {
      while (iter.hasNext()) {
        final BlockMetaData blockMeta = iter.next();
        final long age = Instant.now().getEpochSecond() - blockMeta.getCTime().getSeconds();
        if (age < unit.toSeconds(duration)) {
          continue;
        }
        final Collection<String> blockVolumeIds = blockMeta.getStorageInfo().getVolumeIdList();

        if (blockVolumeIds.size() == 1) {
          final String blockVolumeId = Iterables.getOnlyElement(blockVolumeIds);
          if (srcVolumeId.equals(blockVolumeId)) {
            relocateBlock(blockMeta.getBlockId(), dstVolume);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error", e);
    }
  }

  public void relocateBlock(final BlockIdentifier blockId, final Volume dstVolume) throws IOException {
    final Optional<BlockMetaData> bm = this.blockMetaDataService.getBlockMetaData(blockId, true);

    if (bm.isPresent()) {
      final Path cpyFile =
          this.blockStorageService.copyBlock(blockId, bm.get().getStorageInfo().getVolumeIdList(), dstVolume);

      final StorageInfo.Builder updatedStorageInfo =
          StorageInfo.newBuilder(bm.get().getStorageInfo()).addVolumeId(dstVolume.getId().toString());

      final BlockMetaData updateBlockMetaData =
          BlockMetaData.newBuilder(bm.get()).setStorageInfo(updatedStorageInfo).build();

      // Add to metadata first so if the storage fails, the file will not be
      // orphaned on the volume
      this.blockMetaDataService.updateBlock(updateBlockMetaData);

      this.blockStorageService.finalizeBlock(blockId, cpyFile, dstVolume);
    }
  }

}
