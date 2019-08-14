package io.github.belugabehr.datanode.storage.volume;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.micrometer.core.instrument.MeterRegistry;

@Service
public class VolumeManager {

  @Autowired
  private MeterRegistry meterRegisty;

  @Value("${dn.data.blocks.dir:/data}")
  private Path dataPath;

  private final Map<UUID, Volume> volumeMap = Maps.newHashMap();

  @PostConstruct
  public void init() throws IOException {
    final Collection<Volume> volumes = doDiscoverVolumes(dataPath);
    Preconditions.checkState(!volumes.isEmpty(), "Must be at least one volume");

    volumes.forEach(v -> {
      this.volumeMap.put(v.getUuid(), v);
      this.meterRegisty.counter("datanode.fs.vol.count").increment();
    });
  }

  public Volume getNextAvailableVolume(final long requestedBlockSize) {
    final int volCount = this.volumeMap.size();
    return Iterables.get(this.volumeMap.values(), ThreadLocalRandom.current().nextInt(volCount));
  }

  protected Collection<Volume> doDiscoverVolumes(final Path dataPath) throws IOException {
    final DirectoryStream.Filter<Path> filter = new DirectoryStream.Filter<Path>() {
      public boolean accept(Path file) throws IOException {
        return Files.isDirectory(file);
      }
    };

    final List<Volume> volumes = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dataPath, filter)) {
      for (final Path p : stream) {
        volumes.add(new DefaultVolume(p).init());
      }
    }
    return volumes;
  }

  public Optional<Volume> getVolume(final String volumeUUID) {
    return Optional.fromNullable(this.volumeMap.get(UUID.fromString(volumeUUID)));
  }

  public Collection<Volume> getVolumes() {
    return Collections.unmodifiableCollection(this.volumeMap.values());
  }

}
