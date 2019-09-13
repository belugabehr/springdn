package io.github.belugabehr.datanode;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("datanode.dfs")
public class DfsProperties {
  private List<String> servers;

  public List<String> getServers() {
    return servers;
  }

  public void setServers(List<String> servers) {
    this.servers = servers;
  }

  @Override
  public String toString() {
    return "DfsProperties [servers=" + servers + "]";
  }

}
