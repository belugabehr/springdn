package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

import com.google.common.base.Preconditions;

public class SpreadDirectory {

  private Path rootPath;
  private int levelOne;
  private int levelTwo;

  private SpreadDirectory() {
  }

  public void spread() throws IOException {
    final Collection<Path> paths = getSpreadPaths();
    if (!Files.exists(rootPath)) {
      Files.createDirectories(rootPath);
    }
    for (final Path path : paths) {
      if (!Files.exists(path)) {
        Files.createDirectories(path);
      }
    }
  }

  private Collection<Path> getSpreadPaths() {
    final Collection<Path> paths = new ArrayList<>(this.levelOne + (this.levelOne * this.levelTwo));
    for (int l1 = 0; l1 < this.levelOne; l1++) {
      final Path levelOnePath = this.rootPath.resolve(getHexPath(l1));
      paths.add(levelOnePath);
      for (int l2 = 0; l2 < this.levelTwo; l2++) {
        final Path levelTwoPath = levelOnePath.resolve(getHexPath(l2));
        paths.add(levelTwoPath);
      }
    }
    return paths;
  }

  private Path getHexPath(final int intValue) {
    final String hexStr = String.format("%02X", intValue);
    // Only two hex characters are supported: [1..255]
    return Paths.get(hexStr.substring(hexStr.length() - 2));
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
      Preconditions.checkArgument(levelOne > 0 && levelOne <= 256);
      this.levelOne = levelOne;
      return this;
    }

    public Builder levelTwo(final int levelTwo) {
      Preconditions.checkArgument(levelTwo > 0 && levelTwo <= 256);
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
