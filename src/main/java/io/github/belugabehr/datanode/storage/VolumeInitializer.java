package io.github.belugabehr.datanode.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class VolumeInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeInitializer.class);

  private static final Path ID_FILE = Path.of("volume.id");
  private static final Path TMP_DIR = Path.of("tmp");

  public UUID init(final Path storageDirectory) throws IOException {
    LOG.debug("Initializing directory: {}", storageDirectory);
    Files.createDirectory(storageDirectory);
    final Path idFile = storageDirectory.resolve(ID_FILE);
    final Optional<UUID> volumeGroupId = extractId(idFile);
    if (volumeGroupId.isPresent()) {
      initializeTmpDirectory(storageDirectory);
      return volumeGroupId.get();
    }
    return doInitialize(idFile, storageDirectory);
  }

  private Optional<UUID> extractId(final Path idFile) throws IOException {
    if (!Files.isReadable(idFile)) {
      return Optional.empty();
    }

    final List<String> lines = Files.readAllLines(idFile, StandardCharsets.UTF_8);
    if (lines.size() != 1) {
      return Optional.empty();
    }

    final String idStr = Iterables.getOnlyElement(lines);
    try {
      return Optional.of(UUID.fromString(idStr));
    } catch (IllegalArgumentException iea) {
      LOG.debug("Found invalid volume UUID [{}]. Reinitializing volume.");
      return Optional.empty();
    }
  }

  private UUID doInitialize(final Path idFile, final Path storageDirectory) throws IOException {
    final UUID volumeId = UUID.randomUUID();

    LOG.info("Initializing volume located at: {}", storageDirectory);

    Files.walk(storageDirectory).filter(p -> !p.equals(storageDirectory)).sorted(Comparator.reverseOrder())
        .map(Path::toFile).peek(f -> {
          LOG.debug("Deleting file: [{}]", f);
        }).forEach(File::delete);

    LOG.info("Writing volume ID to file [{}][{}]", volumeId, idFile);
    Files.writeString(idFile, volumeId.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.WRITE);

    initializeTmpDirectory(storageDirectory);

    return volumeId;
  }

  private void initializeTmpDirectory(final Path storageDirectory) throws IOException {
    final Path tmpDir = storageDirectory.resolve(TMP_DIR);

    LOG.info("Initializing volume temp directory: [{}]", tmpDir);

    if (Files.isDirectory(tmpDir)) {
      LOG.info("Deleting existing temp dir: [{}]", tmpDir);
      Files.walk(tmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).peek(f -> {
        LOG.debug("Deleting temp file: [{}]", f);
      }).forEach(File::delete);
    }

    LOG.debug("Creating new tmp directory: [{}]", tmpDir);
    Files.createDirectory(tmpDir);
  }

}
