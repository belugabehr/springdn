package io.github.belugabehr.datanode.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class FixedLengthBlockCheckedOutputStream extends OutputStream {
  private final int blockSize;
  private final ByteBuffer buffer;
  private final ByteArrayOutputStream checksums;
  private final Checksum checksum;
  private long count;

  public FixedLengthBlockCheckedOutputStream() {
    this(512);
  }

  /**
   * TODO: Allow users to pass in an 'expected' total size so that the checksums
   * can be more accurately set
   * 
   * @param blockSize
   */
  public FixedLengthBlockCheckedOutputStream(final int blockSize) {
    this(new CRC32C(), blockSize);
  }

  public FixedLengthBlockCheckedOutputStream(final Checksum checksum, final int blockSize) {
    Preconditions.checkArgument(blockSize > 0);
    this.blockSize = blockSize;
    this.checksum = checksum;
    this.buffer = ByteBuffer.allocate(blockSize);
    this.checksums = new ByteArrayOutputStream(16384);
    this.count = 0L;
  }

  public byte[] getChecksums() {
    return this.checksums.toByteArray();
  }

  public String getChecksumAlgo() {
    return this.checksum.getClass().getSimpleName();
  }

  public int getBlockSize() {
    return this.blockSize;
  }

  @Override
  public void write(final int b) throws IOException {
    this.buffer.put((byte) b);
    this.count++;
    flushIfFull();
  }

  @Override
  public void write(final byte[] b, final int offset, final int length) throws IOException {
    int off = offset;
    int len = length;
    while (len > 0) {
      final int n = Math.min(len, this.buffer.remaining());
      if (n == this.blockSize) {
        this.checksum.update(b, off, n);
        recordChecksum();
      } else {
        this.buffer.put(b, off, n);
        flushIfFull();
      }
      len -= n;
      off += n;
    }
    this.count += length;
  }

  private void flushIfFull() throws IOException {
    if (!buffer.hasRemaining()) {
      flush();
    }
  }

  private void recordChecksum() throws IOException {
    final int checksum32 = (int) (this.checksum.getValue() & 0xffffffff);
    this.checksum.reset();
    this.checksums.write(Ints.toByteArray(checksum32));
  }

  @Override
  public void flush() throws IOException {
    final int bytesBuffered = this.buffer.position();
    if (bytesBuffered > 0) {
      this.buffer.flip();
      this.checksum.update(this.buffer);
      recordChecksum();
      this.buffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    this.flush();
  }

  public long getCount() {
    return this.count;
  }
}
