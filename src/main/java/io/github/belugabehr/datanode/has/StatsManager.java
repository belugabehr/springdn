package io.github.belugabehr.datanode.has;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

@Component
public class StatsManager {

  @Autowired
  private MeterRegistry meterRegisty;

  private Map<String, AtomicLong> volumeStatsMap = new ConcurrentHashMap<>();

  public void blockCount(final String volumeUUID, final String blockPool, final long size) {
    this.volumeStatsMap.computeIfAbsent(volumeUUID, k -> {
      AtomicLong byteCount = new AtomicLong();
      this.meterRegisty.gauge("datanode.fs.vol." + volumeUUID + ".dfs.used", byteCount);
      return byteCount;
    }).addAndGet(size);
  }

}
