/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common.adapters;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ColumnsType;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQLStandardDatatypeImporter;

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

  String getDatabaseProduct();

  String getDbName();

  Object getTables();

  void addTable(String tableName, String tableFolder, String tableDescription);

  void addColumnForTable(String tableName, String columnName, String columnID, String columType,
    String columnOriginalType, String columnDefaultValue, Boolean columnStructureNillable, String columnDescription,
    String lobType);

  Object getSIARDDK();

  PrimaryKey getPrimaryKey(Object primaryKeyXml);

  List<Object> getTable();

  void addPrimaryKeyForTable(String tableName, String primaryKeyName, List<String> escapedColumnNames);

  void addForeignKeyForTable(String tableName, String foreignKeyName, String foreignKeyReferencedTable,
    Map<String, String> escapedReferencedColumns);

  void setRowsForTable(String tableName, BigInteger bigInteger);

  void addView(String viewName, String viewQueryOriginal, String viewDescription);

  void setSiardDiark(Object siardDiark);

  void setArchiveIndex(Object archiveIndex);

  void setTableType(Object tableType);

}
