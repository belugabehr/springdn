package io.github.belugabehr.datanode.storage;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Preconditions;

public class DefaultVolume implements Volume {

  private static final Path TMP_DIR = Path.of("tmp");

  private final Path dataPath;
  private final Path tmpDir;
  private final FileStore fileStore;
  private final LongAdder ioErrorCount;

  private UUID id;

  public DefaultVolume(final UUID id, final Path dataPath) {
    this.id = id;
    this.dataPath = dataPath;
    this.tmpDir = this.dataPath.resolve(TMP_DIR);
    this.fileStore = determineFileStore(dataPath);
    this.ioErrorCount = new LongAdder();

    Preconditions.checkState(Files.isDirectory(this.tmpDir));
  }

  /**
   * Mount point exposed by JDK class UnixMountEntry which is only accessible in
   * toString(). Example: / (/dev/sda1)
   */
  private FileStore determineFileStore(final Path path) {
    final Path absPath = path.toAbsolutePath();
    for (final FileStore fs : FileSystems.getDefault().getFileStores()) {
      final String[] elems = fs.toString().split(" ");
      if (elems.length > 0) {
        final Path mountPath = Paths.get(elems[0]);
        if (absPath.startsWith(mountPath.toAbsolutePath())) {
          return fs;
        }
      }
    }
    throw new RuntimeException("Could not determine path's file system: " + path);
  }

  @Override
  public long getUsableSpace() {
    try {
      return this.fileStore.getUsableSpace();
    } catch (IOException e) {
      return 0L;
    }
  }

  public long getTotalSpace() {
    try {
      return this.fileStore.getTotalSpace();
    } catch (IOException e) {
      return 0L;
    }
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public Path getPath() {
    return this.dataPath;
  }

  @Override
  public Path getTempFile() throws IOException {
    return Files.createTempFile(this.tmpDir, "block-", ".tmp.data");
  }

  @Override
  public void reportError(final IOException ioe) {
    this.ioErrorCount.increment();
  }

  @Override
  public Number getErrors() {
    return Long.valueOf(this.ioErrorCount.sum());
  }

  @Override
  public boolean isFailed() {
    return this.ioErrorCount.sum() > 0L;
  }

  @Override
  public String toString() {
    return "DefaultVolume [dataPath=" + dataPath + ", tmpDir=" + tmpDir + ", fileStore=" + fileStore + ", ioErrorCount="
        + ioErrorCount + ", id=" + id + "]";
  }
}
