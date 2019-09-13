package io.github.belugabehr.datanode.comms.nn.hb;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.belugabehr.datanode.blockpool.BlockPoolManager;
import io.github.belugabehr.datanode.comms.nn.NameNodeConnectionPool;
import io.github.belugabehr.datanode.events.InvalidateBlockListener;
import io.github.belugabehr.datanode.has.Metrics;
import io.github.belugabehr.datanode.storage.StorageManager;
import io.github.belugabehr.datanode.storage.VolumeGroup;
import io.github.belugabehr.datanode.util.HadoopCompatible;
import io.micrometer.core.instrument.MeterRegistry;

public class HeartbeatAction implements Runnable {

  private static Logger LOG = LoggerFactory.getLogger(HeartbeatAction.class);

  private final StorageManager volumeManager;

  private final MeterRegistry meterRegistry;

  private final InvalidateBlockListener invalidateBlockListener;

  private final URI nameNodeURI;

  private final NameNodeConnectionPool connections;

  private final BlockPoolManager blockPoolManager;

  private final String blockPoolID;

  public HeartbeatAction(StorageManager volumeManager, MeterRegistry meterRegistry,
      InvalidateBlockListener invalidateBlockListener, URI nameNodeURI, NameNodeConnectionPool connections,
      BlockPoolManager blockPoolManager, String blockPoolID) {
    this.volumeManager = volumeManager;
    this.meterRegistry = meterRegistry;
    this.invalidateBlockListener = invalidateBlockListener;
    this.nameNodeURI = nameNodeURI;
    this.connections = connections;
    this.blockPoolManager = blockPoolManager;
    this.blockPoolID = blockPoolID;
  }

  @Override
  public void run() {
    try {
      final Collection<VolumeGroup> volumeGroups = this.volumeManager.getVolumeGroups();
      Collection<StorageReport> reports = new ArrayList<>();
      for (final VolumeGroup volumeGroup : volumeGroups) {
        final DatanodeStorage dnStorage =
            new DatanodeStorage(HadoopCompatible.getDatanodeStorageUuid(volumeGroup.getId()));

        final long totalSpace = volumeGroup.getTotalSpace();
        final long usableSpace = volumeGroup.getUsableSpace();
        final boolean isFailed = volumeGroup.isFailed();

        reports.add(new StorageReport(dnStorage, isFailed, totalSpace, 0L, usableSpace, 0L, 0L));
      }

      final StorageReport[] reportsArray = reports.toArray(new StorageReport[0]);

      final int xceiverTotalCount =
          (int) this.meterRegistry.get(Metrics.IPC_XCEIVER_TOTAL_COUNT.registryName()).gauge().value();
      final int xceiverActiveCount =
          (int) this.meterRegistry.get(Metrics.IPC_XCEIVER_ACTIVE_COUNT.registryName()).gauge().value();

      ServerStatus status = new ServerStatus(reportsArray, 0, 0, xceiverActiveCount, xceiverTotalCount, 0, null);

      LOG.info("Sending status: {}", status);

      sendHeartbeat(status);
    } catch (Exception e) {
      LOG.warn("Heartbeat Service error while sending heartbeat", e);
    }
  }

  private void sendHeartbeat(final ServerStatus status) {
    DatanodeProtocolClientSideTranslatorPB connection = null;

    try {
      final DatanodeRegistration registration = blockPoolManager.getDatanodeRegistration(blockPoolID);

      connection = this.connections.borrowObject(nameNodeURI);

      final HeartbeatResponse response = connection.sendHeartbeat(registration, status.getReports(), 0L, 0L, 0, 0, 0,
          null, false, SlowPeerReports.EMPTY_REPORT, SlowDiskReports.EMPTY_REPORT);

      final DatanodeCommand[] commands = response.getCommands();

      for (final DatanodeCommand command : commands) {
        LOG.warn("Command: {}", command);
        switch (command.getAction()) {
        case DatanodeProtocol.DNA_TRANSFER:
          break;
        case DatanodeProtocol.DNA_INVALIDATE:
          final BlockCommand blockCommand = (BlockCommand) command;
          invalidateBlockListener.publish(blockCommand);
          break;
        case DatanodeProtocol.DNA_CACHE:
          break;
        case DatanodeProtocol.DNA_UNCACHE:
          break;
        case DatanodeProtocol.DNA_SHUTDOWN:
          throw new UnsupportedOperationException("Received unimplemented DNA_SHUTDOWN");
        case DatanodeProtocol.DNA_FINALIZE:
          break;
        case DatanodeProtocol.DNA_RECOVERBLOCK:
          break;
        case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
          break;
        case DatanodeProtocol.DNA_BALANCERBANDWIDTHUPDATE:
          break;
        case DatanodeProtocol.DNA_REGISTER:
          break;
        default:
          LOG.warn("Unknown DatanodeCommand action: " + command.getAction());
        }
      }
    } catch (Exception e) {
      LOG.warn("Heartbeat Service error while procesiing command", e);
    } finally {
      try {
        if (connection != null) {
          this.connections.returnObject(nameNodeURI, connection);
        }
      } catch (Exception e) {
      }
    }
  }

}
