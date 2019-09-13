package io.github.belugabehr.datanode.storage;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("datanode.storage")
public class StorageProperties {
  private Map<String, VolumeGroupProperties> groups = new HashMap<>();

  public Map<String, VolumeGroupProperties> getGroups() {
    return groups;
  }

  public void setGroups(Map<String, VolumeGroupProperties> groups) {
    this.groups = groups;
  }

  @Override
  public String toString() {
    return "StorageProperties [groups=" + groups + "]";
  }

}
