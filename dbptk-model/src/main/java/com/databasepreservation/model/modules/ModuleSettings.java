/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules;

import java.util.Set;

import org.apache.commons.lang3.tuple.Triple;

/**
 * Custom settings set by the export module that modify behaviour of the import
 * module.
 *
 * The suggested way to use this class is to create an anonymous class and
 * override the desired settings.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ModuleSettings {
  public ModuleSettings() {
  }

  /**
   * Should the import module retrieve table rows and call handleRow(...) on the
   * export module for each row.
   * 
   * @return the setting choice
   */
  public boolean shouldFetchRows() {
    return true;
  }

  /**
   * Set of tables that should be processed. If null is returned all tables should
   * be processed.
   * 
   * @return The set of tables to process, each entry should be a pair like
   *         Pair(schemaName,tableName). Return `null` to process all tables. An
   *         empty set ignores all tables.
   */
  public Set<Triple<String, String, String>> selectedTables() {
    return null;
  }

  /**
   * Use the selectedTables set to determine if a table is selected.
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @return true if the table is selected, false otherwise
   */
  public boolean isSelectedTable(String schemaName, String tableName) {
    return selectedTables() == null || selectedTables().contains(Triple.of(schemaName, tableName, null));
  }

  /**
   * Use the selectedTables set to determine if a column is selected.
   *
   * @param schemaName
   *          the schema name
   * @param tableName
   *          the table name
   * @param columnName
   *          the column name
   * @return true if the column is selected, false otherwise
   */
  public boolean isSelectedColumn(String schemaName, String tableName, String columnName) {
    return selectedTables() == null || selectedTables().contains(Triple.of(schemaName, tableName, columnName));
  }

  public boolean shouldCountRows() {
    return true;
  }
}
