package io.github.belugabehr.datanode.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("datanode.storage")
public class StorageProperties {
  private final Map<String, StorageDetails> storages = new HashMap<>();

  public Map<String, StorageDetails> getStorages() {
    return Collections.unmodifiableMap(this.storages);
  }

  @Override
  public String toString() {
    return "StorageProperties [storages=" + storages + "]";
  }
}
