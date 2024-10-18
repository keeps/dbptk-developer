/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.path;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.output.SIARDDK128ExportModule;
import org.apache.commons.lang3.NotImplementedException;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK128ContentPathExportStrategy implements ContentPathExportStrategy {

  private static final String CONTENT_DIR = "Tables";
  private static final String TABLE_DIR = "table";
  private static final String TABLE_FILENAME = "table";
  private static final String SCHEMA_DIR = "schema";
  private static final String DOCUMENT_DIR = "Documents";
  private static final String DOC_COLLECTION = "docCollection";
  private static final String fileCount = "1"; // Design decision

  private LOBsTracker lobsTracker;

  public SIARDDK128ContentPathExportStrategy(SIARDDK128ExportModule siarddk128ExportModule) {
    lobsTracker = siarddk128ExportModule.getLobsTracker();
  }

  @Override
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return null;
  }

  /**
   * @return File path of the BLOB but without the file extension since this has
   *         to be determined from the BLOB inputstream
   */
  @Override
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {

    // TO-DO: add test case

    int docCollectionCount = lobsTracker.getDocCollectionCount();
    int LOBsCount = lobsTracker.getLOBsCount();

    // Note: code assumes one file in each folder
    return new StringBuilder().append(DOCUMENT_DIR).append(SIARDDKConstants.FILE_SEPARATOR).append(DOC_COLLECTION)
      .append(docCollectionCount).append(SIARDDKConstants.FILE_SEPARATOR).append(LOBsCount)
      .append(SIARDDKConstants.FILE_SEPARATOR).append(fileCount).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR)
      .toString();
  }

  // Not used in SIARDDK
  @Override
  public String getSchemaFolderName(int schemaIndex) {
    return null;
  }

  @Override
  public String getTableFolderName(int tableIndex) {
    return new StringBuilder().append(TABLE_DIR).append(tableIndex).toString();

  }

  /**
   * @param schemaIndex
   *          not used in SIARDDK
   */
  @Override
  public String getTableXsdFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(SIARDDKConstants.FILE_SEPARATOR)
      .append(getTableFolderName(tableIndex)).append(SIARDDKConstants.FILE_SEPARATOR).append(TABLE_FILENAME)
      .append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION)
      .toString();

  }

  @Override
  public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(SIARDDKConstants.FILE_SEPARATOR)
      .append(getTableFolderName(tableIndex)).append(SIARDDKConstants.FILE_SEPARATOR).append(TABLE_FILENAME)
      .append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XML_EXTENSION)
      .toString();
  }

  @Override
  public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
    return new StringBuilder().append(base).append(SCHEMA_DIR).append(schemaIndex).append("/").append(TABLE_FILENAME)
      .append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION)
      .toString();
  }

  @Override
  public String getTableXsdFileName(int tableIndex) {
    return new StringBuilder().append(TABLE_FILENAME).append(tableIndex)
      .append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION).toString();
  }

  @Override
  public String getColumnFolderName(int columnIndex) {
    throw new NotImplementedException("getColumnFolderName is not used in SIARDDK");
  }
}
