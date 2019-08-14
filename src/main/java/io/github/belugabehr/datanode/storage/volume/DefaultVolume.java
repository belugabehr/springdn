package io.github.belugabehr.datanode.storage.volume;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.github.belugabehr.datanode.util.DataNodeUUIDUtil;

public class DefaultVolume implements Volume {

  private final Path dataPath;
  private final Path tmpDir;
  private final FileStore fileStore;
  private final AtomicInteger ioErrorCount;

  private UUID uuid;

  public DefaultVolume(final Path dataPath) {
    this.dataPath = dataPath;
    this.tmpDir = this.dataPath.resolve("tmp");
    this.fileStore = determineFileStore(dataPath);
    this.ioErrorCount = new AtomicInteger();
  }

  public Volume init() throws IOException {
    this.uuid = DataNodeUUIDUtil.getUUID(dataPath.toString());

    createTempDirectory(this.tmpDir);

    return this;
  }

  private void createTempDirectory(final Path tempDir) throws IOException {
    if (Files.exists(tempDir)) {
      try (Stream<Path> walk = Files.walk(tempDir, 1)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
    Files.createDirectory(tempDir);
  }

  /**
   * Mount point exposed by JDK class UnixMountEntry which is only accessible in
   * toString(). Example: / (/dev/sda1)
   */
  private FileStore determineFileStore(final Path path) {
    final Path absPath = path.toAbsolutePath();
    final Iterable<FileStore> it = FileSystems.getDefault().getFileStores();
    for (final FileStore fs : it) {
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
  public UUID getUuid() {
    return uuid;
  }

  @Override
  public Path getPath() {
    return this.dataPath;
  }

  @Override
  public Path getTempFile() throws IOException {
    // TODO: Pool this so each request does not have to block
    return Files.createTempFile(this.tmpDir, "block-", ".tmp.data");
  }

  @Override
  public void reportError(final IOException ioe) {
    this.ioErrorCount.incrementAndGet();
  }

  @Override
  public Number getErrors() {
    return Integer.valueOf(this.ioErrorCount.get());
  }

  @Override
  public boolean isFailed() {
    return this.ioErrorCount.get() > 0;
  }

  @Override
  public String toString() {
    return "DefaultVolume [dataPath=" + dataPath + ", tmpDir=" + tmpDir + ", fileStore=" + fileStore + ", ioErrorCount="
        + ioErrorCount + ", uuid=" + uuid + "]";
  }
}
