package io.github.belugabehr.datanode.registration;

import java.net.InetAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockPoolInfo;
import com.cloudera.datanode.domain.DataNodeDomain.DataNodeInfo;

import io.github.belugabehr.datanode.blockpool.BlockPoolManager;
import io.github.belugabehr.datanode.comms.nn.NameNodeConnectionPool;
import io.github.belugabehr.datanode.comms.nn.hb.HeartbeatService;
import io.github.belugabehr.datanode.meta.dfs.DfsMetaDataService;

@Service
public class RegistrationService {
  private static final Logger LOG = LoggerFactory.getLogger(RegistrationService.class);

  @Autowired
  private NameNodeConnectionPool connections;

  @Autowired
  private DfsMetaDataService dfsMetaDataService;

  @Autowired
  private BlockPoolManager blockPoolManager;
  
  @Autowired
  private HeartbeatService heartbeatService;

  public void register() throws Exception {
    final DataNodeInfo dataNodeInfo = this.dfsMetaDataService.getDataNodeInfo();
    final Collection<URI> namenodeURIs = this.dfsMetaDataService.getNameNodeURIs();

    for (final URI namenodeURI : namenodeURIs) {
      DatanodeProtocolClientSideTranslatorPB comms = this.connections.borrowObject(namenodeURI);
      try {
        final NamespaceInfo namespace = comms.versionRequest();
        final String clusterID = namespace.getClusterID();
        final int namespaceID = namespace.getNamespaceID();
        final int layoutVersion = namespace.getLayoutVersion();
        final long cTime = namespace.getCTime();
        final String blockPoolId = namespace.getBlockPoolID();
//        namespace.getBuildVersion();
//        namespace.getSoftwareVersion();

        final Optional<BlockPoolInfo> bpInfoOpt = this.blockPoolManager.getBlockPoolInfo(blockPoolId);
        final BlockPoolInfo bpInfo;
        if (bpInfoOpt.isPresent()) {
          bpInfo = bpInfoOpt.get();
        } else {
          this.blockPoolManager.initialize(blockPoolId);
          final BlockPoolInfo newBlockPoolInfo =
              BlockPoolInfo.newBuilder().setBlockPoolID(blockPoolId).setClusterID(clusterID).setNamespaceID(namespaceID)
                  .setLayoutVersion(layoutVersion).setCTime(cTime).build();
          this.blockPoolManager.addBlockPoolInfo(newBlockPoolInfo);
          bpInfo = newBlockPoolInfo;
        }
        
//        checkNSEquality(bpNSInfo.getBlockPoolID(), nsInfo.getBlockPoolID(),
//            "Blockpool ID");
//        checkNSEquality(bpNSInfo.getNamespaceID(), nsInfo.getNamespaceID(),
//            "Namespace ID");
//        checkNSEquality(bpNSInfo.getClusterID(), nsInfo.getClusterID(),
//            "Cluster ID");

        final StorageInfo storageInfo = new StorageInfo(bpInfo.getLayoutVersion(), bpInfo.getNamespaceID(),
            bpInfo.getClusterID(), bpInfo.getCTime(), NodeType.DATA_NODE);

        final InetAddress localhost = this.dfsMetaDataService.getInetAddress();
        final DatanodeID dnid =
            new DatanodeID(localhost.getHostAddress(), localhost.getHostName(), dataNodeInfo.getDataNodeUUID(),
                this.dfsMetaDataService.getDataTransferPort(), this.dfsMetaDataService.getInfoPort(), 0, 0);

        final DatanodeRegistration reg =
            new DatanodeRegistration(dnid, storageInfo, new ExportedBlockKeys(), VersionInfo.getVersion());

        final DatanodeRegistration registration = comms.registerDatanode(reg);
        registration.setNamespaceInfo(namespace);

        LOG.info("{}", registration);

        this.blockPoolManager.associate(blockPoolId, registration);
        this.blockPoolManager.associate(blockPoolId, namenodeURI);
        this.heartbeatService.add(namenodeURI, blockPoolId);

      } finally {
        this.connections.returnObject(namenodeURI, comms);
      }
    }
  }
}
