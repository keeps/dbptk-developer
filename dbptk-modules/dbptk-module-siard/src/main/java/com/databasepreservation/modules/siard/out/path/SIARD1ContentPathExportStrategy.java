/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.path;

/**
 * Defines a SIARD 1.0 implementation to get paths to folders and files
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ContentPathExportStrategy implements ContentPathExportStrategy {
  // names for directories
  private static final String CONTENT_DIR = "content";
  private static final String SCHEMA_DIR = "schema";
  private static final String TABLE_DIR = "table";
  private static final String LOB_DIR = "lob";

  // names for files
  private static final String TABLE_FILENAME = "table";
  private static final String LOB_FILENAME = "record";

  // extensions for files
  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";
  public static final String CLOB_EXTENSION = "txt";
  public static final String BLOB_EXTENSION = "bin";

  // control characters
  private static final String RESOURCE_FILE_SEPARATOR = "/";
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  @Override
  public String getInternalClobFileName(int rowIndex) {
    return new StringBuilder().append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(CLOB_EXTENSION).toString();
  }

  @Override
  public String getInternalBlobFileName(int rowIndex) {
    return new StringBuilder().append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(BLOB_EXTENSION).toString();
  }

  @Override
  public String getRelativeInternalLobDirPath(int schemaIndex, int tableIndex, int columnIndex) {
    return new StringBuilder().append(SCHEMA_DIR).append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR)
      .append(tableIndex).append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR).append(columnIndex).toString();
  }

  @Override
  public String getAbsoluteInternalLobDirPath(int schemaIndex, int tableIndex, int columnIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR)
      .append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR).append(columnIndex).toString();
  }

  @Override
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR)
      .append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR).append(columnIndex).append(RESOURCE_FILE_SEPARATOR)
      .append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR).append(CLOB_EXTENSION).toString();
  }

  @Override
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR)
      .append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR).append(columnIndex).append(RESOURCE_FILE_SEPARATOR)
      .append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR).append(BLOB_EXTENSION).toString();
  }

  @Override
  public String getSchemaFolderName(int schemaIndex) {
    return new StringBuilder().append(SCHEMA_DIR).append(schemaIndex).toString();
  }

  @Override
  public String getTableFolderName(int tableIndex) {
    return new StringBuilder().append(TABLE_DIR).append(tableIndex).toString();
  }

  @Override
  public String getTableXsdFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR)
      .append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XSD_EXTENSION).toString();
  }

  @Override
  public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR)
      .append(schemaIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XML_EXTENSION).toString();
  }

  @Override
  public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
    return new StringBuilder().append(base).append(SCHEMA_DIR).append(schemaIndex).append(RESOURCE_FILE_SEPARATOR)
      .append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
  }

  @Override
  public String getTableXsdFileName(int tableIndex) {
    return new StringBuilder().append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XSD_EXTENSION).toString();
  }

  @Override
  public String getColumnFolderName(int columnIndex) {
    return new StringBuilder().append(LOB_DIR).append(columnIndex).toString();
  }
}
