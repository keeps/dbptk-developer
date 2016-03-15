package com.databasepreservation.modules.siard.out.path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentPathExportStrategy implements ContentPathExportStrategy {
  // names for directories
  protected static final String CONTENT_DIR = "content";
  protected static final String SCHEMA_DIR = "schema";
  protected static final String TABLE_DIR = "table";
  protected static final String LOB_DIR = "lob";

  // names for files
  protected static final String TABLE_FILENAME = "table";
  protected static final String LOB_FILENAME = "record";

  // extensions for files
  protected static final String XML_EXTENSION = "xml";
  protected static final String XSD_EXTENSION = "xsd";
  protected static final String CLOB_EXTENSION = "txt";
  protected static final String BLOB_EXTENSION = "bin";

  // control characters
  protected static final String RESOURCE_FILE_SEPARATOR = "/";
  protected static final String FILE_EXTENSION_SEPARATOR = ".";

  public String getColumnFolderName(int columnIndex) {
    return new StringBuilder().append(LOB_DIR).append(columnIndex).toString();
  }

  public String getClobFileName(int rowIndex) {
    return new StringBuilder().append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(CLOB_EXTENSION).toString();
  }

  public String getBlobFileName(int rowIndex) {
    return new StringBuilder().append(LOB_FILENAME).append(rowIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(BLOB_EXTENSION).toString();
  }

  @Override
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR)
      .append(columnIndex).append(RESOURCE_FILE_SEPARATOR).append(LOB_FILENAME).append(rowIndex)
      .append(FILE_EXTENSION_SEPARATOR).append(CLOB_EXTENSION).toString();
  }

  @Override
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(RESOURCE_FILE_SEPARATOR).append(LOB_DIR)
      .append(columnIndex).append(RESOURCE_FILE_SEPARATOR).append(LOB_FILENAME).append(rowIndex)
      .append(FILE_EXTENSION_SEPARATOR).append(BLOB_EXTENSION).toString();
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
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_FILENAME)
      .append(tableIndex).append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
  }

  @Override
  public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_DIR).append(schemaIndex)
      .append(RESOURCE_FILE_SEPARATOR).append(TABLE_DIR).append(tableIndex).append(RESOURCE_FILE_SEPARATOR).append(TABLE_FILENAME)
      .append(tableIndex).append(FILE_EXTENSION_SEPARATOR).append(XML_EXTENSION).toString();
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
}
