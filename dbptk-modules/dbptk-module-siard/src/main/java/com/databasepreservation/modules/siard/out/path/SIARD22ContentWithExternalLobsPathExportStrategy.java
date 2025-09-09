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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */

public class SIARD22ContentWithExternalLobsPathExportStrategy extends SIARD22ContentPathExportStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD22ContentWithExternalLobsPathExportStrategy.class);

  public static final String FILE_SEPARATOR = File.separator;

  // schema, table, column -> current segment
  private Map<Triple<Integer, Integer, Integer>, Integer> currentDatabaseSegments = new HashMap<>();

  public Path nextContainerBasePath(Path mainContainerBasePath, Triple<Integer, Integer, Integer> segmentKey) {
    int currentSegmentIndex;
    if (currentDatabaseSegments.containsKey(segmentKey)) {
      currentSegmentIndex = currentDatabaseSegments.get(segmentKey) + 1;
    } else {
      currentSegmentIndex = 0;
    }
    currentDatabaseSegments.put(segmentKey, currentSegmentIndex);
    return Paths
      .get(mainContainerBasePath.toAbsolutePath().toString() + "_lobs" + FILE_SEPARATOR + "s" + segmentKey.getLeft()
        + "_t" + segmentKey.getMiddle() + "_c" + segmentKey.getRight() + FILE_SEPARATOR + "seg_" + currentSegmentIndex);
  }

  public String getClobOuterFilePath(int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append("t").append(tableIndex).append("_c").append(columnIndex).append("_r")
      .append(rowIndex).append(FILE_EXTENSION_SEPARATOR).append(CLOB_EXTENSION).toString();
  }

  public String getBlobOuterFilePath(int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append("t").append(tableIndex).append("_c").append(columnIndex).append("_r")
      .append(rowIndex).append(FILE_EXTENSION_SEPARATOR).append(BLOB_EXTENSION).toString();
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
