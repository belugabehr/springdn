package io.github.belugabehr.datanode.meta.block;

import com.google.common.hash.Hashing;

public enum BlockMetaKeys {
  BLOCK_META((byte) 'm'), BLOCK_CHECKSUM((byte) 'c');

  private static final byte KEY_DELIM = 0x00;

  private final byte keyPrefix;

  private BlockMetaKeys(final byte keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public byte[] getKeyValue(final byte[] value) {
    final byte[] key = new byte[1 + 1 + 20];

    key[0] = this.keyPrefix;
    key[1] = KEY_DELIM;

    Hashing.sha1().hashBytes(value).writeBytesTo(key, 2, 20);

    return key;
  }

  public byte getKeyPrefix() {
    return this.keyPrefix;
  }

  public boolean includesKey(final byte[] key) {
    return key[0] == this.keyPrefix;
  }
}
