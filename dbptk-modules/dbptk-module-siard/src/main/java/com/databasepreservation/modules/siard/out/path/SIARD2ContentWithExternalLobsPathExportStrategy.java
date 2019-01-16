/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.path;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentWithExternalLobsPathExportStrategy extends SIARD2ContentPathExportStrategy {
  public static final String FILE_SEPARATOR = File.separator;
  private long externalContainerId = 0L;

  public Path nextContainerBasePath(Path mainContainerBasePath) {
    externalContainerId++;
    return Paths.get(mainContainerBasePath.toAbsolutePath().toString() + "_lobseg_" + externalContainerId);
  }

  @Override
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(FILE_SEPARATOR).append(LOB_DIR)
      .append(columnIndex).append(FILE_SEPARATOR).append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(CLOB_EXTENSION).toString();
  }

  @Override
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(FILE_SEPARATOR).append(LOB_DIR)
      .append(columnIndex).append(FILE_SEPARATOR).append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(BLOB_EXTENSION).toString();
  }
}
