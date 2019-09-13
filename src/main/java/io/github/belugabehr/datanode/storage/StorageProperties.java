package io.github.belugabehr.datanode.storage;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("datanode.storage")
public class StorageProperties {
  private Map<String, StorageDetails> placement = new HashMap<>();

  public Map<String, StorageDetails> getPlacement() {
    return placement;
  }

  public void setPlacement(Map<String, StorageDetails> placement) {
    this.placement = placement;
  }

  @Override
  public String toString() {
    return "StorageProperties [placement=" + placement + "]";
  }

}
