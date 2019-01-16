/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentPathImportStrategy {
  String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName);

  /**
   * Stores the relationship between the schema and it's folder
   * 
   * @param schemaName
   *          the schema name
   * @param schemaFolder
   *          the folder name
   */
  void associateSchemaWithFolder(String schemaName, String schemaFolder);

  /**
   * Stores the relationship between the table and it's folder
   * 
   * @param tableId
   *          the table id
   * @param tableFolder
   *          the folder name
   */
  void associateTableWithFolder(String tableId, String tableFolder);

  /**
   * Stores the relationship between the column and it's folder
   * 
   * @param columnId
   *          the column id
   * @param columnFolder
   *          the folder name
   */
  void associateColumnWithFolder(String columnId, String columnFolder);

  /**
   * Returns the path to the table.xml file given a schema name and a table id
   * 
   * @param schemaName
   *          the schema name
   * @param tableId
   *          the table id
   * @return the tableN.xml file path
   * @throws ModuleException
   *           schemaName is null; tableId is null; the schema does not have an
   *           associated folder (set with associateSchemaWithFolder); the table
   *           does not have an associated folder (set with
   *           associateTableWithFolder)
   */
  String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException;

  /**
   * Returns the path to the table.xsd file given a schema name and a table id
   *
   * @param schemaName
   *          the schema name
   * @param tableId
   *          the table id
   * @return the tableN.xsd file path
   * @throws ModuleException
   *           schemaName is null; tableId is null; the schema does not have an
   *           associated folder (set with associateSchemaWithFolder); the table
   *           does not have an associated folder (set with
   *           associateTableWithFolder)
   */
  String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException;
}
