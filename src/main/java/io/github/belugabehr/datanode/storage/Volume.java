package io.github.belugabehr.datanode.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

public interface Volume {
  long getUsableSpace();

  long getTotalSpace();

  Path getPath();

  UUID getId();

  Path getTempFile() throws IOException;

  void reportError(IOException ioe);

  Number getErrors();
  
  boolean isFailed();
}
