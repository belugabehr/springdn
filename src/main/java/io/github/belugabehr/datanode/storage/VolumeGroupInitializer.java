package io.github.belugabehr.datanode.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

@Component
public class VolumeGroupInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeGroupInitializer.class);

  private static final Path ID_FILE = Path.of("volumegroup.id");

  public UUID init(final Path mountDirectory) throws IOException {
    Preconditions.checkArgument(Files.isDirectory(mountDirectory), "All configured storage directories must exist");

    final Path idFile = mountDirectory.resolve(ID_FILE);
    final Optional<UUID> volumeGroupId = extractId(idFile);
    if (volumeGroupId.isPresent()) {
      return volumeGroupId.get();
    }
    return doInitialize(idFile);
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
      LOG.debug("Found invalid volume group UUID [{}]. Reinitializing volume group.");
      return Optional.empty();
    }
  }

  private UUID doInitialize(final Path idFile) throws IOException {
    final UUID volumeGroupId = UUID.randomUUID();

    LOG.info("Writing volume group UUID [{}][{}]", volumeGroupId, idFile);
    Files.writeString(idFile, volumeGroupId.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.WRITE);

    return volumeGroupId;
  }

}
