package io.github.belugabehr.datanode.stats;

import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.springframework.stereotype.Service;

@Service
public class DefaultStatsManager implements StatsManager
{

    @Override
    public StorageReport getStorageReport(final String storageID)
    {
//        final DatanodeStorage storage = new DatanodeStorage(storageID);
//        return new StorageReport(storage, false, new File("/").getTotalSpace(), 0, new File("/").getUsableSpace(), 0);
    	return null;
    }

}
