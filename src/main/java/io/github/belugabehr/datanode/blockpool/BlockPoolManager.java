package io.github.belugabehr.datanode.blockpool;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockPoolInfo;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.github.belugabehr.datanode.meta.dfs.DfsMetaDataService;
import io.github.belugabehr.datanode.storage.StorageManager;
import io.github.belugabehr.datanode.storage.Volume;

@Service
public class BlockPoolManager {

  @Autowired
  private DfsMetaDataService dfsMetaDataService;

  @Autowired
  private StorageManager volumeManager;

  @Autowired
  private BlockPoolInitializer blockPoolInitializer;

  private final Map<String, DatanodeRegistration> registrations = new ConcurrentHashMap<>();

  private final Multimap<String, URI> blockPoolNameNodeMap = ArrayListMultimap.create();

  public BlockPoolInfo register(final NamespaceInfo namespace) throws IOException {
    final Optional<BlockPoolInfo> bpInfo = this.dfsMetaDataService.getBlockPoolInfo(namespace.getBlockPoolID());

    if (bpInfo.isPresent()) {
      return bpInfo.get();
    }

    final Collection<Volume> volumes = volumeManager.getAllVolumes();
    this.blockPoolInitializer.init(namespace.getBlockPoolID(), volumes);

    final BlockPoolInfo newBlockPoolInfo = BlockPoolInfo.newBuilder().setClusterID(namespace.getClusterID())
        .setNamespaceID(namespace.getNamespaceID()).setBlockPoolID(namespace.getBlockPoolID())
        .setLayoutVersion(namespace.getLayoutVersion()).setCTime(namespace.getCTime()).build();

    this.dfsMetaDataService.addBlockPoolInfo(newBlockPoolInfo);

    return newBlockPoolInfo;
  }

  public void associate(String blockPoolId, DatanodeRegistration registration) {
    this.registrations.put(blockPoolId, registration);
  }

  public DatanodeRegistration getDatanodeRegistration(final String blockPoolID) {
    return this.registrations.get(blockPoolID);
  }

  public void associate(String blockPoolId, URI namenodeURI) {
    blockPoolNameNodeMap.put(blockPoolId, namenodeURI);
  }

  public Collection<URI> getNameNodes(final String blockpoolId) {
    return Collections.unmodifiableCollection(this.blockPoolNameNodeMap.get(blockpoolId));
  }
}
