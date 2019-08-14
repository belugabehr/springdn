package io.github.belugabehr.datanode.util;

import java.util.UUID;

public final class HadoopCompatible {

  private HadoopCompatible() {
  }

  public static String getDatanodeStorageUuid(final String plainUuid) {
    return "DS-" + plainUuid;
  }

  public static String getDatanodeStorageUuid(final UUID uuid) {
    return getDatanodeStorageUuid(uuid.toString());
  }
}
