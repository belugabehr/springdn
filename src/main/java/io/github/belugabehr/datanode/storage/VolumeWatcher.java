package io.github.belugabehr.datanode.storage;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

@Component
public class VolumeWatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeWatcher.class);

  @Autowired
  @Qualifier("globalScheduledTaskExecutor")
  private ScheduledExecutorService scheduler;

  @Autowired
  private VolumeInitializer volumeInitializer;

  private Path rootDataPath;

  private WatchKey watchKey;

  public void watch(final Path rootDataPath) throws IOException {
    Preconditions.checkState(this.rootDataPath == null, "Cannot watch more than one path");

    final WatchService watcher = rootDataPath.getFileSystem().newWatchService();
    this.watchKey = rootDataPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

    this.rootDataPath = rootDataPath;

    this.scheduler.scheduleWithFixedDelay(this, 5L, 5L, TimeUnit.MINUTES);

  }

  @Override
  public void run() {
    for (final WatchEvent<?> event : this.watchKey.pollEvents()) {
      final WatchEvent.Kind<?> kind = event.kind();

      // An OVERFLOW event can occur regardless if events are lost or discarded.
      if (kind == OVERFLOW) {
        continue;
      }

      // The filename is the context of the event.
      @SuppressWarnings("unchecked")
      final WatchEvent<Path> ev = (WatchEvent<Path>) event;
      final Path filename = ev.context();

      final Path child = this.rootDataPath.resolve(filename);

      try {
        final boolean isDirectory = Files.isDirectory(child);
        LOG.info("Detected new file in mount directory [{}][directory:{}]", child, isDirectory);

        if (Files.isDirectory(child)) {
          this.volumeInitializer.init(child);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // Reset the key -- this step is critical if you want to
    // receive further watch events. If the key is no longer valid,
    // the directory is inaccessible so exit the loop.
    final boolean valid = this.watchKey.reset();
    if (!valid) {
      LOG.error("Root data directory [{}] is not longer valid. Restart service.", this.rootDataPath);
    }
  }

}
