package dk.magenta.siarddk;

import java.io.File;

import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;

public class SIARDDKContentExportPathStrategy implements ContentPathExportStrategy {

  private static final String CONTENT_DIR = "Tables";
  private static final String TABLE_DIR = "table";
  private static final String TABLE_FILENAME = "table";
  private static final String SCHEMA_DIR = "schema";

  private static final String FILE_SEPARATOR = File.separator;
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";

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
    return new StringBuilder().append(CONTENT_DIR).append(FILE_SEPARATOR).append(getTableFolderName(tableIndex))
      .append(FILE_SEPARATOR).append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XSD_EXTENSION).toString();

  }

  @Override
  public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
    return new StringBuilder().append(CONTENT_DIR).append(FILE_SEPARATOR).append(getTableFolderName(tableIndex))
      .append(FILE_SEPARATOR).append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XML_EXTENSION).toString();
  }

  @Override
  public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
    return new StringBuilder().append(base).append(SCHEMA_DIR).append(schemaIndex).append(FILE_SEPARATOR)
      .append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
  }

  @Override
  public String getTableXsdFileName(int tableIndex) {
    return new StringBuilder().append(TABLE_FILENAME).append(tableIndex).append(FILE_EXTENSION_SEPARATOR)
      .append(XSD_EXTENSION).toString();
  }

}
