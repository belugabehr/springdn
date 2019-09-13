package io.github.belugabehr.datanode.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

@Service
public class StorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(StorageManager.class);

  @Autowired
  private VolumeWatcher volumeWatcher;

  @Autowired
  private MeterRegistry meterRegisty;

  @Autowired
  private StorageProperties storageProperties;

  @Autowired
  private VolumeGroupInitializer volumeGroupInitializer;

  @Autowired
  private VolumeInitializer volumeInitializer;

  private final Map<UUID, VolumeGroup> volumeGroups = Maps.newHashMap();

  private final Map<UUID, Volume> volumes = Maps.newHashMap();

  @PostConstruct
  public void init() throws IOException {
    for (final Entry<String, VolumeGroupProperties> entry : this.storageProperties.getGroups().entrySet()) {
      LOG.info("Processing storage: {} [{}]", entry.getKey(), entry.getValue().getDescription());

      final String pathStr = entry.getValue().getDirectory();
      final Path storageDirectory = Paths.get(pathStr);

      final Map<UUID, Volume> volumeMap = Maps.newHashMap();

      final UUID volumeGroupId = this.volumeGroupInitializer.init(storageDirectory);

      final Collection<Path> availableVolumes = doDiscoverVolumes(storageDirectory);
      availableVolumes.forEach(v -> {
        try {
          final UUID volumeId = this.volumeInitializer.init(v);
          final Volume volume = new DefaultVolume(volumeId, v);
          volumeMap.put(volumeId, volume);
        } catch (IOException ioe) {
          LOG.error("Could not initialize and register a volume [{}]. Skipped.", v, ioe);
        }
      });

      this.volumes.putAll(volumeMap);

      final VolumeGroup volumeGroup = new VolumeGroup();
      volumeGroup.setId(volumeGroupId);
      volumeGroup.setName(entry.getKey());
      volumeGroup.setDescription(entry.getValue().getDescription());
      volumeGroup.setVolumes(volumeMap);

      this.volumeGroups.put(volumeGroupId, volumeGroup);

      this.volumeWatcher.watch(storageDirectory);
    }

    this.meterRegisty.gaugeCollectionSize("datanode.fs.vol.group.count", Tags.empty(), this.volumeGroups.keySet());
  }

  public Volume getNextAvailableVolume(final UUID volumeGroupId, final long requestedBlockSize) {
    final Collection<Volume> volumes = this.volumeGroups.get(volumeGroupId).getVolumes().values();
    final int volCount = volumes.size();
    return Iterables.get(volumes, ThreadLocalRandom.current().nextInt(volCount));
  }

  protected Collection<Path> doDiscoverVolumes(final Path dataPath) throws IOException {
    return Files.list(dataPath).filter(Files::isDirectory).collect(Collectors.toList());
  }

  public Optional<Volume> getVolume(final UUID volumeUUID) {
    return Optional.fromNullable(this.volumes.get(volumeUUID));
  }

  public Collection<VolumeGroup> getVolumeGroups() {
    return Collections.unmodifiableCollection(this.volumeGroups.values());
  }

  public Collection<Volume> getAllVolumes() {
    return this.volumeGroups.values().stream().map(volumeGroup -> volumeGroup.getVolumes())
        .flatMap(entry -> entry.values().stream()).filter(volume -> !volume.isFailed()).collect(Collectors.toList());
  }

}
