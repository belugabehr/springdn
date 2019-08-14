package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;

import io.github.belugabehr.datanode.storage.volume.Volume;

public class WatchedFileChannel extends FileChannel {

  private final FileChannel channel;
  private final Volume volume;

  public WatchedFileChannel(final Volume volume, final FileChannel channel) {
    Preconditions.checkNotNull(volume);
    Preconditions.checkNotNull(channel);

    this.volume = volume;
    this.channel = channel;
  }

  public static WatchedFileChannel watch(final Volume volume, final FileChannel fileChannel) {
    return new WatchedFileChannel(volume, fileChannel);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    try {
      channel.force(metaData);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public FileLock lock(long position, long size, boolean shared) throws IOException {
    try {
      return channel.lock(position, size, shared);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public MappedByteBuffer map(MapMode arg0, long arg1, long arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() throws IOException {
    try {
      return channel.position();
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    try {
      channel.position(newPosition);
      return this;
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    try {
      return channel.read(dst);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    try {
      return channel.read(dst, position);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    try {
      return channel.read(dsts, offset, length);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public long size() throws IOException {
    try {
      return channel.size();
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public long transferFrom(ReadableByteChannel arg0, long arg1, long arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long transferTo(long arg0, long arg1, WritableByteChannel arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileChannel truncate(long size) throws IOException {
    try {
      channel.truncate(size);
      return this;
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public FileLock tryLock(long arg0, long arg1, boolean arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    try {
      return channel.write(src);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    try {
      return channel.write(src, position);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    try {
      return channel.write(srcs, offset, length);
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }

  @Override
  protected void implCloseChannel() throws IOException {
    try {
      channel.close();
    } catch (IOException ioe) {
      this.volume.reportError(ioe);
      throw ioe;
    }
  }
}
