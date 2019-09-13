package io.github.belugabehr.datanode;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("datanode.ipc.transfer")
public class IpcProperties {
  private int port;

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public String toString() {
    return "IpcProperties [port=" + port + "]";
  }

}
