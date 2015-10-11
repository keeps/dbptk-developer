package com.databasepreservation.modules.siard.out.path;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKContentPathExportStrategy implements ContentPathExportStrategy {

  private static final String CONTENT_DIR = "Tables";
  private static final String TABLE_DIR = "table";
  private static final String TABLE_FILENAME = "table";
  private static final String SCHEMA_DIR = "schema";

  @Override
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return null;
  }

  @Override
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    // TODO Auto-generated method stub
    return null;
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
      .append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XSD_EXTENSION).toString();

  }

  @Override
  public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(SIARDDKConstants.FILE_SEPARATOR)
      .append(getTableFolderName(tableIndex)).append(SIARDDKConstants.FILE_SEPARATOR).append(TABLE_FILENAME)
      .append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR).append(SIARDDKConstants.XML_EXTENSION).toString();
  }

  @Override
  public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
    return new StringBuilder().append(base).append(SCHEMA_DIR).append(schemaIndex).append(SIARDDKConstants.FILE_SEPARATOR)
      .append(TABLE_FILENAME).append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR)
      .append(SIARDDKConstants.XSD_EXTENSION).toString();
  }

  @Override
  public String getTableXsdFileName(int tableIndex) {
    return new StringBuilder().append(TABLE_FILENAME).append(tableIndex).append(SIARDDKConstants.FILE_EXTENSION_SEPARATOR)
      .append(SIARDDKConstants.XSD_EXTENSION).toString();
  }

}
