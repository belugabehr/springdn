package io.github.belugabehr.datanode.meta.block;

import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DBIterator;

import com.cloudera.datanode.domain.DataNodeDomain.BlockMetaData;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class BlockMetaIterator implements Iterator<BlockMetaData>, AutoCloseable {

  private final DBIterator dbIter;
  private boolean hasNext;
  private Entry<byte[], byte[]> nextEntry;

  public BlockMetaIterator(final DBIterator dbIter) {
    this.hasNext = true;
    this.dbIter = dbIter;
    this.dbIter.seek(new byte[] { BlockMetaKeys.BLOCK_META.getKeyPrefix() });
  }

  @Override
  public void close() throws Exception {
    this.dbIter.close();
  }

  @Override
  public boolean hasNext() {
    if (this.hasNext = this.dbIter.hasNext()) {
      this.nextEntry = this.dbIter.next();
      if (!BlockMetaKeys.BLOCK_META.includesKey(this.nextEntry.getKey())) {
        this.hasNext = false;
        this.nextEntry = null;
      }
    }

    return this.hasNext;
  }

  @Override
  public BlockMetaData next() {
    Preconditions.checkState(this.nextEntry != null);
    try {
      return BlockMetaData.parseFrom(this.nextEntry.getValue());
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

}
