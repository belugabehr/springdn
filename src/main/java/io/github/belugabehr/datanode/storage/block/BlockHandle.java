package io.github.belugabehr.datanode.storage.block;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.storage.Volume;

public class BlockHandle implements AutoCloseable {

  private final BlockIdentifier blockID;
  private final Volume volume;
  private final FileChannel channel;
  private final Path path;

  public BlockHandle(BlockIdentifier blockID, Volume volume, FileChannel channel, Path path) {
    this.blockID = blockID;
    this.volume = volume;
    this.channel = channel;
    this.path = path;
  }

  @Override
  public void close() throws Exception {
    this.channel.close();
  }

  public BlockIdentifier getBlockId() {
    return this.blockID;
  }

  public Volume getVolume() {
    return this.volume;
  }

  public FileChannel getFileChannel() {
    return this.channel;
  }

  public Path getPath() {
    return path;
  }

}
