package io.github.belugabehr.datanode.util.ex;

import java.io.IOException;

public class BlockNotFoundException extends IOException {
  public BlockNotFoundException() {
    super("Could not find block in metastore");
  }
}
