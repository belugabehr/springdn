package io.github.belugabehr.datanode.comms.nn.hb;

public enum Metrics {

  IPC_XCEIVER_TOTAL_COUNT("datanode.ipc.xceiverCount"), IPC_XCEIVER_ACTIVE_COUNT("datanode.ipc.xceiverActiveCount");

  private final String metricName;

  private Metrics(final String metricName) {
    this.metricName = metricName;
  }

  public String registryName() {
    return this.metricName;
  }

}
