package io.github.belugabehr.datanode.comms.nn.hb;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.github.belugabehr.datanode.blockpool.BlockPoolManager;
import io.github.belugabehr.datanode.comms.nn.NameNodeConnectionPool;
import io.github.belugabehr.datanode.events.InvalidateBlockListener;
import io.github.belugabehr.datanode.storage.StorageManager;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class HeartbeatService {

  private static Logger LOG = LoggerFactory.getLogger(HeartbeatService.class);

  @Autowired
  private StorageManager volumeManager;

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private InvalidateBlockListener invalidateBlockListener;

  @Autowired
  private NameNodeConnectionPool connections;

  @Autowired
  private BlockPoolManager blockPoolManager;

  @Autowired
  @Qualifier("globalScheduledTaskExecutor")
  private ScheduledExecutorService scheduler;

  @Value("${dn.heartbeat.frequency:6}")
  private int heartbeatFrequency;

  public void add(URI namenodeURI, String blockPoolId) {
    HeartbeatAction a = new HeartbeatAction(volumeManager, meterRegistry, invalidateBlockListener, namenodeURI,
        connections, blockPoolManager, blockPoolId);
    this.scheduler.scheduleAtFixedRate(a, 0L, heartbeatFrequency, TimeUnit.SECONDS);
  }
}
