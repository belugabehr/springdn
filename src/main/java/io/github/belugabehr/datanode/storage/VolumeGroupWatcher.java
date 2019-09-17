package io.github.belugabehr.datanode.storage;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VolumeGroupWatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeGroupWatcher.class);

  private final VolumeGroup volumeGroup;
  private final ScheduledExecutorService executorService;
  private final VolumeGroupChangeListener listener;

  private WatchKey watchKey;

  public VolumeGroupWatcher(final VolumeGroup volumeGroup, final ScheduledExecutorService executorService,
      final VolumeGroupChangeListener listener) {
    Objects.requireNonNull(volumeGroup);
    Objects.requireNonNull(executorService);
    Objects.requireNonNull(listener);

    this.volumeGroup = volumeGroup;
    this.executorService = executorService;
    this.listener = listener;
  }

  public VolumeGroupWatcher watch() throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    this.watchKey = this.volumeGroup.getMountDirectory().register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

    this.executorService.scheduleWithFixedDelay(this, 1L, 1L, TimeUnit.MINUTES);

    return this;
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

      final Path child = this.volumeGroup.getMountDirectory().resolve(filename);

      final boolean isDirectory = Files.isDirectory(child);
      LOG.info("Detected new file in mount directory [{}][isDirectory:{}]", child, isDirectory);

      if (kind == ENTRY_CREATE) {
        try {
          this.listener.volumeAdded(this.volumeGroup, child);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    // Reset the key -- this step is critical if you want to
    // receive further watch events. If the key is no longer valid,
    // the directory is inaccessible so exit the loop.
    final boolean valid = this.watchKey.reset();
    if (!valid) {
      LOG.error("Volume Group mount directory [{}] is not longer valid. Restart service.", this.volumeGroup);
    }
  }

}
