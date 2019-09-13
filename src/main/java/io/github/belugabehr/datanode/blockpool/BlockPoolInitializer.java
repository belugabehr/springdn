package io.github.belugabehr.datanode.blockpool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.github.belugabehr.datanode.storage.Volume;
import io.github.belugabehr.datanode.util.SpreadDirectory;

@Component
public class BlockPoolInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(BlockPoolInitializer.class);

  public void init(final String blockPoolId, final Collection<Volume> volumes) {
    LOG.info("Initializing new block pool: [{}]", blockPoolId);

    for (final Volume volume : volumes) {
      final Path volumeRoot = volume.getPath();
      final Path blockPoolRoot = volumeRoot.resolve(blockPoolId);

      LOG.debug("Initializing block pool [{}] on volume [{}]", blockPoolId, volumeRoot);

      try {
        final SpreadDirectory spreadDirectory =
            SpreadDirectory.newBuilder().rootPath(blockPoolRoot).levelOne(16).levelTwo(256).build();

        spreadDirectory.spread();
      } catch (IOException ioe) {
        volume.reportError(ioe);
      }
    }
  }

}
