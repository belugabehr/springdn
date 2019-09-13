package io.github.belugabehr.datanode.storage;

public class VolumeGroupProperties {

  private String description;
  private String directory;

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

  @Override
  public String toString() {
    return "StorageDetails [description=" + description + ", directory=" + directory + "]";
  }

}
