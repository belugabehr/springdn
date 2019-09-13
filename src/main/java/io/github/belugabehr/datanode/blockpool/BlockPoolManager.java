package io.github.belugabehr.datanode.blockpool;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockPoolInfo;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.github.belugabehr.datanode.meta.dfs.DfsMetaDataService;
import io.github.belugabehr.datanode.storage.StorageManager;
import io.github.belugabehr.datanode.storage.Volume;
import io.github.belugabehr.datanode.util.SpreadDirectory;

@Service
public class BlockPoolManager {

  @Autowired
  private DfsMetaDataService dfsMetaDataService;

  @Autowired
  private StorageManager volumeManager;

  private final Map<String, DatanodeRegistration> registrations = new ConcurrentHashMap<>();

  private final Multimap<String, URI> blockPoolNameNodeMap = ArrayListMultimap.create();

  public Optional<BlockPoolInfo> getBlockPoolInfo(final String blockPoolId) {
    final BlockPoolInfo bpi = this.dfsMetaDataService.getBlockPoolInfo(blockPoolId);
    return Optional.ofNullable(bpi);
  }

  public void initialize(final String blockPoolId) {
    final Collection<Volume> volumes = this.volumeManager.getVolumes();
    for (final Volume volume : volumes) {
      final Path volumeRoot = volume.getPath();
      final Path blockPoolRoot = volumeRoot.resolve(blockPoolId);

      final SpreadDirectory spreadDirectory =
          SpreadDirectory.newBuilder().rootPath(blockPoolRoot).levelOne(16).levelTwo(256).build();

      try {
        spreadDirectory.spread();
      } catch (IOException ioe) {
        volume.reportError(ioe);
      }
    }
  }

  public void addBlockPoolInfo(BlockPoolInfo newBlockPoolInfo) {
    this.dfsMetaDataService.addBlockPoolInfo(newBlockPoolInfo);
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
