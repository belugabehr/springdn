package io.github.belugabehr.datanode.stats;

import org.apache.hadoop.hdfs.server.protocol.StorageReport;

public interface StatsManager
{
    StorageReport getStorageReport(String storageID);
}
