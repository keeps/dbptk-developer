/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.path;

/**
 * Interface to describe paths to folders and files for some SIARD archive.
 * <p>
 * Paths to files should NOT end with a slash and are relative to the root of
 * the SIARD archive Namespaces should have a prefix and some parameters to
 * build a URL Names are names of folders or files (for files it should also
 * contain the extension)
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentPathExportStrategy {
  /**
   * Returns a LOB file's name
   *
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   * @param rowIndex
   *          Row index (begins at 0)
   */
  public String getInternalClobFileName(int rowIndex);

  /**
   * Returns a LOB file's name
   *
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   * @param rowIndex
   *          Row index (begins at 0)
   */
  public String getInternalBlobFileName(int rowIndex);

  /**
   * Returns the directory path where a column's LOB files are stored relative to
   * the archive's lob folder
   *
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   */
  public String getRelativeInternalLobDirPath(int schemaIndex, int tableIndex, int columnIndex);

  /**
   * Returns the directory path where a column's LOB files are stored
   * 
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   */
  public String getAbsoluteInternalLobDirPath(int schemaIndex, int tableIndex, int columnIndex);

  /**
   * Returns the path to a LOB file
   *
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   * @param rowIndex
   *          Row index (begins at 0)
   */
  public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex);

  /**
   * Returns the path to a LOB file
   *
   * @param schemaIndex
   *          Schema index (begins at 1)
   * @param tableIndex
   *          Table index (begins at 1)
   * @param columnIndex
   *          Column index (begins at 1)
   * @param rowIndex
   *          Row index (begins at 0)
   */
  public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex);

  /**
   * Returns the name of the database schema folder
   *
   * @param schemaIndex
   *          database schema index (begins at 1)
   */
  public String getSchemaFolderName(int schemaIndex);

  /**
   * Returns the name of the table column folder
   *
   * @param columnIndex
   *          table column index (begins at 1)
   */
  public String getColumnFolderName(int columnIndex);

  /**
   * Returns the name of a table's folder
   *
   * @param tableIndex
   *          table index (begins at 1)
   */
  public String getTableFolderName(int tableIndex);

  /**
   * Returns the path to a table's XML file
   *
   * @param schemaIndex
   *          database schema index (begins at 1)
   * @param tableIndex
   *          table index (begins at 1)
   */
  public String getTableXsdFilePath(int schemaIndex, int tableIndex);

  /**
   * Returns the path to a table's XSD file
   *
   * @param schemaIndex
   *          database schema index (begins at 1)
   * @param tableIndex
   *          table index (begins at 1)
   */
  public String getTableXmlFilePath(int schemaIndex, int tableIndex);

  /**
   * Returns the XML schema URL to use in XML namespace declaration
   *
   * @param schemaIndex
   *          database schema index (begins at 1)
   * @param tableIndex
   *          table index (begins at 1)
   */
  public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex);

  /**
   * Returns the name of the XML schema file for the specified table
   *
   * @param tableIndex
   *          table index (begins at 1)
   */
  public String getTableXsdFileName(int tableIndex);
}
