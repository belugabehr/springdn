package io.github.belugabehr.datanode.comms.nn.hb;

import java.util.Arrays;

import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;

public class ServerStatus {

  private final StorageReport[] reports;
  private final long cacheCapacity;
  private final long cacheUsed;
  private final int xmitsInProgress;
  private final int xceiverCount;
  private final int failedVolumes;
  private final VolumeFailureSummary volumeFailureSummary;

  public ServerStatus(StorageReport[] reports, long cacheCapacity, long cacheUsed, int xmitsInProgress,
      int xceiverCount, int failedVolumes, VolumeFailureSummary volumeFailureSummary) {
    this.reports = reports;
    this.cacheCapacity = cacheCapacity;
    this.cacheUsed = cacheUsed;
    this.xmitsInProgress = xmitsInProgress;
    this.xceiverCount = xceiverCount;
    this.failedVolumes = failedVolumes;
    this.volumeFailureSummary = volumeFailureSummary;
  }

  public StorageReport[] getReports() {
    return reports;
  }

  public long getCacheCapacity() {
    return cacheCapacity;
  }

  public long getCacheUsed() {
    return cacheUsed;
  }

  public int getXmitsInProgress() {
    return xmitsInProgress;
  }

  public int getXceiverCount() {
    return xceiverCount;
  }

  public int getFailedVolumes() {
    return failedVolumes;
  }

  public VolumeFailureSummary getVolumeFailureSummary() {
    return volumeFailureSummary;
  }

  @Override
  public String toString() {
    return "ServerStatus [reports=" + Arrays.toString(reports) + ", cacheCapacity=" + cacheCapacity + ", cacheUsed="
        + cacheUsed + ", xmitsInProgress=" + xmitsInProgress + ", xceiverCount=" + xceiverCount + ", failedVolumes="
        + failedVolumes + ", volumeFailureSummary=" + volumeFailureSummary + "]";
  }

}
