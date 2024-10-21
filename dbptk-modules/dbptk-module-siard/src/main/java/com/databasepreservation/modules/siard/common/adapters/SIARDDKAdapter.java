package com.databasepreservation.modules.siard.common.adapters;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public interface SIARDDKAdapter {
  void setVersion(String value);

  void setDbName(String dbOriginalName);

  void setDatabaseProduct(String productName);

  void addTable(String tableName, String tableFolder, String tableDescription);

  void addColumnForTable(String tableName, String columnName, String columnID, String columType,
    String columnOriginalType, String columnDefaultValue, Boolean columnStructureNillable, String columnDescription,
    String lobType);

  Object getSIARDDK();

  void addPrimaryKeyForTable(String tableName, String primaryKeyName, List<String> escapedColumnNames);

  void addForeignKeyForTable(String tableName, String foreignKeyName, String foreignKeyReferencedTable,
    Map<String, String> escapedReferencedColumns);

  void setRowsForTable(String tableName, BigInteger bigInteger);

  void addView(String viewName, String viewQueryOriginal, String viewDescription);
}
