package io.github.belugabehr.datanode.storage;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;

public class VolumeGroup {

  private UUID id;
  private String name;
  private String description;
  private Path mountDirectory;
  private double reservedSpace;
  private Map<UUID, Volume> volumes = Maps.newConcurrentMap();

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

  public void setMountDirectory(Path mountDirectory) {
    this.mountDirectory = mountDirectory;
  }

  public Path getMountDirectory() {
    return this.mountDirectory;

  }

  public double getReservedSpace() {
    return reservedSpace;
  }

  public void setReservedSpace(double reservedSpace) {
    this.reservedSpace = reservedSpace;
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
    for (final Volume volume : this.volumes.values()) {
      if (!volume.isFailed()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "VolumeGroup [id=" + id + ", name=" + name + ", description=" + description + ", mountDirectory="
        + mountDirectory + ", reservedSpace=" + reservedSpace + ", volumes=" + volumes + "]";
  }

  @Override
  public int hashCode() {
    return (id == null) ? 0 : id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VolumeGroup other = (VolumeGroup) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }

}
