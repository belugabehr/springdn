package io.github.belugabehr.datanode.has;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockMetaData;
import io.github.belugabehr.datanode.meta.block.BlockMetaDataService;
import io.github.belugabehr.datanode.meta.block.BlockMetaIterator;

@Component
public class BlockStatsInitializer {

  @Autowired
  private StatsManager statsManager;

  @Autowired
  private BlockMetaDataService blockMetaDataService;

  public void init() throws Exception {
    try (final BlockMetaIterator blockMetas = this.blockMetaDataService.getBlockMetaData()) {
      while (blockMetas.hasNext()) {
        final BlockMetaData blockMeta = blockMetas.next();
        this.statsManager.blockCount(blockMeta.getStorageInfo().getVolumeId(),
            blockMeta.getBlockId().getBlockPoolId(), blockMeta.getStorageInfo().getBlockSize());
      }
    }
  }

}
