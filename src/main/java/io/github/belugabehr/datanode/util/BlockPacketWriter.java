package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBuf;

public class BlockPacketWriter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockPacketWriter.class);

  /** Assigned Variables */

  private FileChannel channel;
  private IntBuffer checksums;

  private int checksumChunkSize;
  private int targetPayloadSize;
  private int blockReadBytes;
  private int blockReadOffset;

  /** Derived Variables */

  private int startFilePosition;
  private int startChecksumChunkIndex;
  private int packetCount;
  private int chunkCount;
  private int chunksPerPacket;

  /** Internal State Variables */

  private int chunksRemaining;

  private BlockPacketWriter() {
  }

  private BlockPacketWriter init() {

    this.chunksPerPacket = this.targetPayloadSize / (this.checksumChunkSize + Ints.BYTES);

    this.startChecksumChunkIndex = this.blockReadOffset / this.checksumChunkSize;

    final int offsetInChunk = this.blockReadOffset % this.checksumChunkSize;

    this.startFilePosition = this.blockReadOffset - offsetInChunk;

    this.chunksRemaining = this.chunkCount =
        ((this.blockReadBytes + offsetInChunk) + (this.checksumChunkSize - 1)) / this.checksumChunkSize;

    this.packetCount = (this.chunkCount + (this.chunksPerPacket - 1)) / this.chunksPerPacket;

    return this;
  }

  private BlockPacketWriter position() throws IOException {
    this.channel.position(this.startFilePosition);
    this.checksums.position(this.startChecksumChunkIndex);
    return this;
  }

  public int getPacketCount() {
    return this.packetCount;
  }

  public long getFilePosition() throws IOException {
    return this.channel.position();
  }

  public int getNextPacketPayloadLength() throws IOException {
    return Math.toIntExact(Math.min((this.channel.size() - this.channel.position()), (long) getActualPayloadSize()));
  }

  public int getActualPayloadSize() {
    return this.chunksPerPacket * this.checksumChunkSize;
  }

  public int getTargetPayloadSize() {
    return this.targetPayloadSize;
  }
  
  public int write(final ByteBuf bbOut) throws IOException {

    if (this.chunksRemaining == 0) {
      throw new IOException("No more packets to write");
    }

    // First: write the checksums
    final int chunksToWrite = Math.min(this.chunksPerPacket, this.chunksRemaining);
    final int checksumLength = chunksToWrite * Ints.BYTES;
    for (int c = 0; c < chunksToWrite; c++) {
      bbOut.writeInt(this.checksums.get());
    }

    LOG.debug("Wrote {} chunks with a total size of {}b", chunksToWrite, checksumLength);

    // Second: write the Data
    final long dataLength = getNextPacketPayloadLength();
    final int fileTransferCount = bbOut.writeBytes(this.channel, Math.toIntExact(dataLength));
    Preconditions.checkState(fileTransferCount == dataLength);

    LOG.debug("Transferred {}b data to packet buffer", dataLength, checksumLength);

    this.chunksRemaining -= chunksToWrite;

    return Ints.BYTES + fileTransferCount + checksumLength;
  }

  @Override
  public void close() throws Exception {
    this.channel.close();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private FileChannel channel;
    private int targetPayloadSize;
    private byte[] checksums;
    private int checksumChunkSize;
    private int readOffset;
    private int readLength;
    private boolean doPosition;

    private Builder() {
      this.doPosition = true;
    }

    public Builder source(final FileChannel channel) {
      this.channel = channel;
      return this;
    }

    public Builder checksums(final byte[] checksums) {
      this.checksums = checksums;
      return this;
    }

    public Builder checksumChunkSize(final int checksumChunkSize) {
      this.checksumChunkSize = checksumChunkSize;
      return this;
    }

    public Builder targetPayloadSize(final int targetPayloadSize) {
      this.targetPayloadSize = targetPayloadSize;
      return this;
    }

    public Builder readOffset(final int readOffset) {
      this.readOffset = readOffset;
      return this;
    }

    public Builder readLength(final int readLength) {
      this.readLength = readLength;
      return this;
    }

    /** For testing */
    public Builder doPosition(final boolean doPosition) {
      this.doPosition = doPosition;
      return this;
    }

    public BlockPacketWriter build() throws IOException {
      final BlockPacketWriter ps = new BlockPacketWriter();
      ps.channel = this.channel;
      ps.checksums = ByteBuffer.wrap(this.checksums).asIntBuffer();
      ps.checksumChunkSize = this.checksumChunkSize;
      ps.targetPayloadSize = this.targetPayloadSize;
      ps.blockReadBytes = this.readLength;
      ps.blockReadOffset = this.readOffset;
      return this.doPosition ? ps.init().position() : ps.init();
    }
  }
}
