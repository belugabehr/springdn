package io.github.belugabehr.datanode.storage.block;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.stereotype.Component;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.github.belugabehr.datanode.domain.DataNodeDomain.BlockIdentifier;
import io.github.belugabehr.datanode.storage.Volume;

@Component
public class BlockPlacementPolicy {

  private static final String FILE_SUFFIX = ".data";
  private final HashFunction hf = Hashing.md5();

  public Path generateBlockPath(final Volume volume, final BlockIdentifier blockID) throws IOException {
    final HashCode hc = hf.newHasher().putLong(blockID.getBlockId()).hash();
    final String hashHexString = hc.toString().toUpperCase();
    final String levelOne = "0" + hashHexString.substring(0, 1);
    final String levelTwo = hashHexString.substring(1, 3);

    final String fileName = blockID.getBlockId() + "." + blockID.getGenerationStamp() + FILE_SUFFIX;

    return volume.getPath().resolve(blockID.getBlockPoolId()).resolve(Paths.get(levelOne, levelTwo)).resolve(fileName);
  }

}
