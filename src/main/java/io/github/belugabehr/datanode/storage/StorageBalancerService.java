package io.github.belugabehr.datanode.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.github.belugabehr.datanode.storage.block.BlockManager;

@Service
public class StorageBalancerService implements Runnable {

  private static final double MIN_RANGE = 0.10;

  @Autowired
  @Qualifier("globalScheduledTaskExecutor")
  private ScheduledExecutorService executorService;

  @Autowired
  private StorageManager storageManager;

  @Autowired
  private BlockManager blockManager;

  @PostConstruct
  public void init() {
    this.executorService.scheduleWithFixedDelay(this, 1L, 1L, TimeUnit.MINUTES);
  }

  @Override
  public void run() {
    final Collection<VolumeGroup> volumeGroups = this.storageManager.getVolumeGroups();

    final Collection<VolumeGroup> eligibleVolumeGroups =
        volumeGroups.stream().filter(vg -> vg.getVolumes().size() > 1).collect(Collectors.toList());

    for (final VolumeGroup volumeGroup : eligibleVolumeGroups) {
      final List<Volume> volumes = Lists.newArrayList(volumeGroup.getVolumes().values());

      Collections.sort(volumes, new Comparator<Volume>() {

        @Override
        public int compare(Volume v1, Volume v2) {
          final double used1 = (double) v1.getUsableSpace() / (double) v1.getTotalSpace();
          final double used2 = (double) v2.getUsableSpace() / (double) v2.getTotalSpace();
          return Double.compare(used1, used2);
        }
      });

      final Volume maxUtilization = Iterables.getLast(volumes);
      final Volume minUtilization = volumes.iterator().next();

      if (!triggersBlockMove(maxUtilization, minUtilization)) {
        continue;
      }

      blockManager.relocateAnyBlock(maxUtilization, minUtilization);
    }
  }

  protected boolean triggersBlockMove(final Volume v1, final Volume v2) {
    final double used1 = (double) v1.getUsableSpace() / (double) v1.getTotalSpace();
    final double used2 = (double) v2.getUsableSpace() / (double) v2.getTotalSpace();

    return (Math.abs(used1 - used2) > MIN_RANGE);
  }
}
