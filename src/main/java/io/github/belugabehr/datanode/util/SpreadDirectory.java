package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class SpreadDirectory {

  private Path rootPath;
  private int levelOne;
  private int levelTwo;

  private SpreadDirectory() {
  }

  public void spread() throws IOException {
    for (int l1 = 0; l1 < levelOne; l1++) {
      final String levelOneHexStr = String.format("%02X", l1);
      final Path levelOnePath = this.rootPath.resolve(levelOneHexStr.substring(levelOneHexStr.length() - 2));
      Files.createDirectories(levelOnePath);
      for (int l2 = 0; l2 < levelTwo; l2++) {
        final String levelTwoHexStr = String.format("%02X", l2);
        final Path levelTwoPath = levelOnePath.resolve(levelTwoHexStr.substring(levelTwoHexStr.length() - 2));
        Files.createDirectories(levelTwoPath);
      }
    }
  }

  public static class Builder {
    private Path rootPath;
    private int levelOne;
    private int levelTwo;

    public Builder rootPath(final Path rootPath) {
      this.rootPath = rootPath;
      return this;
    }

    public Builder levelOne(final int levelOne) {
      this.levelOne = levelOne;
      return this;
    }

    public Builder levelTwo(final int levelTwo) {
      this.levelTwo = levelTwo;
      return this;
    }

    public SpreadDirectory build() {
      SpreadDirectory spreadDir = new SpreadDirectory();
      spreadDir.rootPath = this.rootPath;
      spreadDir.levelOne = this.levelOne;
      spreadDir.levelTwo = this.levelTwo;
      return spreadDir;
    }

  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
