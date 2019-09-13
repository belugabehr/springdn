package io.github.belugabehr.datanode.storage;

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

public class VolumeGroup {

  private UUID id;
  private String name;
  private String description;
  private Map<UUID, Volume> volumes = Maps.newHashMap();

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Map<UUID, Volume> getVolumes() {
    return volumes;
  }

  public void setVolumes(Map<UUID, Volume> volumes) {
    this.volumes = volumes;
  }

  @Override
  public String toString() {
    return "VolumeGroup [id=" + id + ", name=" + name + ", description=" + description + ", volumes=" + volumes + "]";
  }

  public long getTotalSpace() {
    long totalSpace = 0L;
    for (final Volume volume : this.volumes.values()) {
      totalSpace += volume.getTotalSpace();
    }
    return totalSpace;
  }

  public long getUsableSpace() {
    long usableSpace = 0L;
    for (final Volume volume : this.volumes.values()) {
      usableSpace += volume.getUsableSpace();
    }
    return usableSpace;
  }

  /**
   * Check if this volume group has failed. A volume group is considered failed
   * if it has no volumes associated with it or all the associated volumes are
   * in a failed state.
   *
   * @return true if this volume group is in a failed state; false otherwise
   */
  public boolean isFailed() {
    if (this.volumes.isEmpty()) {
      return true;
    }
    for (final Volume volume : this.volumes.values()) {
      if (!volume.isFailed()) {
        return false;
      }
    }
    return true;
  }

}
