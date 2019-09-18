package io.github.belugabehr.datanode.storage;

public class VolumeGroupProperties {

  private String description;
  private String directory;
  private double reserved;

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public double getReserved() {
    return reserved;
  }

  public void setReserved(double reserved) {
    this.reserved = reserved;
  }

  @Override
  public String toString() {
    return "VolumeGroupProperties [description=" + description + ", directory=" + directory + ", reserved=" + reserved
        + "]";
  }

}
