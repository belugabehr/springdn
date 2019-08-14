package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Preconditions;

public final class DataNodeUUIDUtil {

  private DataNodeUUIDUtil() {
  }

  public synchronized static UUID getUUID(final String dataDirPath) throws IOException {
    final UUID dataNodeUUID;
    final Path rootDirPath = Paths.get(dataDirPath);
    final List<Path> idFiles = getIdFiles(rootDirPath);
    Preconditions.checkState(idFiles.size() <= 1, "More than one UUID file found");

    if (idFiles.isEmpty()) {
      dataNodeUUID = createIdFile(rootDirPath, UUID.randomUUID());
    } else {
      dataNodeUUID = parseFileName(idFiles.get(0));
    }

    return dataNodeUUID;
  }

  private static UUID parseFileName(final Path idFilePath) {
    final String fileName = FilenameUtils.getBaseName(idFilePath.toString());
    final UUID uuid = UUID.fromString(fileName);
    return uuid;
  }

  private static UUID createIdFile(final Path path, final UUID uuid) throws IOException {
    final Path idFilePath = path.resolve(Paths.get(uuid.toString() + ".id"));
    Files.createFile(idFilePath);
    return uuid;
  }

  private static List<Path> getIdFiles(final Path path) throws IOException {
    final DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
      public boolean accept(Path file) throws IOException {
        return file.toString().endsWith(".id");
      }
    };

    List<Path> idFiles = new ArrayList<>(1);
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(path, filter)) {
      stream.forEach(p -> idFiles.add(p));
    }
    return idFiles;
  }
}
