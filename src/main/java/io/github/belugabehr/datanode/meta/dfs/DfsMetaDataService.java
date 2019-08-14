package io.github.belugabehr.datanode.meta.dfs;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB.DBException;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.cloudera.datanode.domain.DataNodeDomain.BlockPoolInfo;
import com.cloudera.datanode.domain.DataNodeDomain.DataNodeInfo;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

@Service
public class DfsMetaDataService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DfsMetaDataService.class);

  @Value("${dn.meta.dir:/var/lib/springdn}")
  private String dataDir;

  @Value("${dn.namenode.servers}")
  private List<String> namenodes;

  @Value("${dn.ipc.transfer.port:51515}")
  private int dataTransferPort;

  @Value("${server.port:8080}")
  private int infoPort;

  private Collection<URI> namenodeURIs;

  private DB db;

  private InetAddress inetAddress;

  @PostConstruct
  public void init() throws Exception {
    Preconditions.checkNotNull(namenodes);
    Preconditions.checkState(!namenodes.isEmpty());

    final List<URI> nnul = new ArrayList<>();
    for (final String nameNode : this.namenodes) {
      nnul.add(new URI(nameNode));
    }
    this.namenodeURIs = nnul;

    final Path metaDir = Paths.get(dataDir, "dfs");
    Files.createDirectories(metaDir);
    this.db = JniDBFactory.factory.open(metaDir.toFile(),
        new Options().paranoidChecks(true).compressionType(CompressionType.SNAPPY));

    this.inetAddress = InetAddress.getLocalHost();

    computeUuidIfAbsent();
  }

  public Collection<URI> getNameNodeURIs() {
    return this.namenodeURIs;
  }

  public int getDataTransferPort() {
    return dataTransferPort;
  }

  public int getInfoPort() {
    return infoPort;
  }

  public InetAddress getInetAddress() {
    return this.inetAddress;
  }

  public DataNodeInfo getDataNodeInfo() {
    final byte[] results = this.db.get("dni".getBytes());
    try {
      return DataNodeInfo.parseFrom(results);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  private void computeUuidIfAbsent() throws DBException {
    final byte[] results = this.db.get("dni".getBytes());
    if (results == null) {
      final DataNodeInfo dni =
          DataNodeInfo.newBuilder().setCTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
              .setDataNodeUUID(UUID.randomUUID().toString()).build();
      this.db.put("dni".getBytes(), dni.toByteArray());
    }
  }

  public BlockPoolInfo getBlockPoolInfo(final String blockPoolId) {
    final byte[] key = ("bp:" + blockPoolId).getBytes();
    final byte[] value = this.db.get(key);
    if (value == null) {
      return null;
    }
    try {
      return BlockPoolInfo.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  public void addBlockPoolInfo(final BlockPoolInfo newBlockPoolInfo) {
    final byte[] key = ("bp:" + newBlockPoolInfo.getBlockPoolID()).getBytes();
    final byte[] value = newBlockPoolInfo.toByteArray();
    this.db.put(key, value);
  }

  @PreDestroy
  @Override
  public void close() throws IOException {
    this.db.close();
  }
}
