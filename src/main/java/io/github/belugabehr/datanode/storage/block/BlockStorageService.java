package io.github.belugabehr.datanode.storage.block;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockIdentifier;
import com.google.common.base.Optional;

import io.github.belugabehr.datanode.storage.StorageManager;
import io.github.belugabehr.datanode.storage.Volume;
import io.github.belugabehr.datanode.util.WatchedFileChannel;
import io.github.belugabehr.datanode.util.ex.BlockNotFoundException;

/**
 * Responsible for the physical storage of blocks.
 */
@Service
public class BlockStorageService {

  private static final Logger LOG = LoggerFactory.getLogger(BlockStorageService.class);

  @Autowired
  private StorageManager volumeManager;

  @Autowired
  private BlockPlacementPolicy placementPolicy;

  public BlockHandle initializeBlock(final UUID volumeGroupId, final BlockIdentifier blockID, final long blockSize) throws Exception {
    final Volume volume = this.volumeManager.getNextAvailableVolume(volumeGroupId, blockSize);

    final Path blockTmpFilePath = volume.getTempFile();

    @SuppressWarnings("resource")
    final RandomAccessFile rf = new RandomAccessFile(blockTmpFilePath.toFile(), "rw");
    final FileChannel channel = WatchedFileChannel.watch(volume, rf.getChannel());

    return new BlockHandle(blockID, volume, channel, blockTmpFilePath);
  }

  /**
   * @param handle
   * @param offset
   * @param data
   * @throws IOException
   */
  public void appendBlock(final BlockHandle handle, final long offset, final ByteBuffer data) throws IOException {
    handle.getFileChannel().position(offset).write(data);
  }

  public FileChannel openBlock(final BlockIdentifier blockID, final String volumeUUID) throws IOException {
    final Optional<Volume> volume = this.volumeManager.getVolume(UUID.fromString(volumeUUID));
    if (volume.isPresent()) {
      final Path finalBlockPath = this.placementPolicy.generateBlockPath(volume.get(), blockID);
      try {
        @SuppressWarnings("resource")
        final RandomAccessFile rf = new RandomAccessFile(finalBlockPath.toFile(), "rw");
        return WatchedFileChannel.watch(volume.get(), rf.getChannel());
      } catch (IOException ioe) {
        volume.get().reportError(ioe);
        throw ioe;
      }
    }
    throw new BlockNotFoundException();
  }

  public void finalizeBlock(final BlockHandle handle, final int bytesWritten) throws IOException {
    try (FileChannel channel = handle.getFileChannel()) {
      channel.truncate(bytesWritten);
    }

    final BlockIdentifier blockID = handle.getBlockID();
    final Path tmpBlockPath = handle.getPath();
    final Path finalBlockPath = this.placementPolicy.generateBlockPath(handle.getVolume(), blockID);

    LOG.debug("Moving tmp block file [{}] to final destination [{}]", tmpBlockPath, finalBlockPath);

    try {
      Files.move(tmpBlockPath, finalBlockPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ioe) {
      handle.getVolume().reportError(ioe);
      throw ioe;
    }
  }

  public void deleteBlock(final BlockIdentifier blockID, final String volumeUUID) throws IOException {
    LOG.info("Delete from volume [{}]: block {}", volumeUUID, blockID);
    final Optional<Volume> volume = this.volumeManager.getVolume(UUID.fromString(volumeUUID));
    if (volume.isPresent()) {
      final Path finalBlockPath = this.placementPolicy.generateBlockPath(volume.get(), blockID);
      try {
        Files.delete(finalBlockPath);
      } catch (NoSuchFileException nsfe) {
        LOG.info("Attempting to delete file which is already deleted: {}", blockID);
      } catch (IOException ioe) {
        volume.get().reportError(ioe);
        throw ioe;
      }
    }
  }
}
