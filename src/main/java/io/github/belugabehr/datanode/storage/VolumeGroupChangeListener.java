package io.github.belugabehr.datanode.storage;

import java.io.IOException;
import java.nio.file.Path;

public interface VolumeGroupChangeListener {

  void volumeAdded(VolumeGroup volumeGroup, Path child) throws IOException;

}
