package com.databasepreservation.model.modules;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

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
   * Set of tables that should be processed. If null is returned all tables
   * should be processed.
   * 
   * @return The set of tables to process, each entry should be a pair like
   *         Pair(schemaName,tableName). Return `null` to process all tables. An
   *         empty set ignores all tables.
   */
  public Set<Pair<String, String>> selectedTables() {
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
    if (selectedTables() == null) {
      return true;
    }

    for (Pair<String, String> pair : selectedTables()) {
      String pairSchemaName = pair.getLeft();
      String pairTableName = pair.getRight();

      if (pairSchemaName.equals(schemaName) && pairTableName.equals(tableName)) {
        return true;
      }
    }
    return false;
  }
}
