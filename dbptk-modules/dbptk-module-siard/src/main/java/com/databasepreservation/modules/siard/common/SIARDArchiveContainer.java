/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common;

import java.nio.file.Path;

import com.databasepreservation.modules.siard.constants.SIARDConstants;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDArchiveContainer {
  private final Path path;
  private final OutputContainerType type;
  private SIARDConstants.SiardVersion version;

  public SIARDArchiveContainer(Path path, OutputContainerType type) {
    this.path = path;
    this.type = type;
  }

  public Path getPath() {
    return path;
  }

  public OutputContainerType getType() {
    return type;
  }

  public SIARDConstants.SiardVersion getVersion() {
    return version;
  }

  public void setVersion(SIARDConstants.SiardVersion version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return new StringBuilder("Container(Type: ").append(type.toString()).append(", Path: '").append(path.toString())
      .append("')").toString();
  }

  public enum OutputContainerType {
    MAIN, AUXILIARY
  }
}
