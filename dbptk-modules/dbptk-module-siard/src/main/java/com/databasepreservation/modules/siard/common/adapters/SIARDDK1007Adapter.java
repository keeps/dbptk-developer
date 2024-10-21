package com.databasepreservation.modules.siard.common.adapters;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

import dk.sa.xmlns.diark._1_0.tableindex.ColumnType;
import dk.sa.xmlns.diark._1_0.tableindex.ColumnsType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ForeignKeysType;
import dk.sa.xmlns.diark._1_0.tableindex.FunctionalDescriptionType;
import dk.sa.xmlns.diark._1_0.tableindex.PrimaryKeyType;
import dk.sa.xmlns.diark._1_0.tableindex.ReferenceType;
import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;
import dk.sa.xmlns.diark._1_0.tableindex.TableType;
import dk.sa.xmlns.diark._1_0.tableindex.TablesType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewType;
import dk.sa.xmlns.diark._1_0.tableindex.ViewsType;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class SIARDDK1007Adapter implements SIARDDKAdapter {
  private final SiardDiark siardDiark;
  private final TablesType tablesType;
  private final ViewsType viewsType;

  public SIARDDK1007Adapter() {
    siardDiark = new SiardDiark();
    tablesType = new TablesType();
    viewsType = new ViewsType();

    siardDiark.setTables(tablesType);
    siardDiark.setViews(viewsType);
  }

  @Override
  public void setVersion(String value) {
    siardDiark.setVersion(value);
  }

  @Override
  public void setDbName(String dbOriginalName) {
    siardDiark.setDbName(dbOriginalName);
  }

  @Override
  public void setDatabaseProduct(String productName) {
    siardDiark.setDatabaseProduct(productName);
  }

  @Override
  public void addTable(String tableName, String tableFolder, String tableDescription) {
    TableType tableType = new TableType();
    tableType.setName(tableName);
    tableType.setFolder(tableFolder);
    tableType.setDescription(tableDescription);
    tableType.setColumns(new ColumnsType());
    tablesType.getTable().add(tableType);
  }

  @Override
  public void addColumnForTable(String tableName, String columnName, String columnID, String columType,
    String columnOriginalType, String columnDefaultValue, Boolean columnStructureNillable, String columnDescription,
    String lobType) {
    ColumnType columnType = new ColumnType();
    columnType.setName(columnName);
    columnType.setColumnID(columnID);
    columnType.setType(columType);
    columnType.setTypeOriginal(columnOriginalType);
    columnType.setDefaultValue(columnDefaultValue);
    columnType.setNullable(columnStructureNillable);
    columnType.setDescription(columnDescription);

    if (lobType != null) {
      if (lobType.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
        FunctionalDescriptionType functionalDescriptionType = FunctionalDescriptionType.DOKUMENTIDENTIFIKATION;
        columnType.getFunctionalDescription().add(functionalDescriptionType);
      }
    }
    tablesType.getTable().stream().filter(table -> table.getName().equals(tableName)).findFirst().get().getColumns()
      .getColumn().add(columnType);
  }

  @Override
  public Object getSIARDDK() {
    return siardDiark;
  }

  @Override
  public void addPrimaryKeyForTable(String tableName, String primaryKeyName, List<String> escapedColumnNames) {
    PrimaryKeyType primaryKeyType = new PrimaryKeyType();
    primaryKeyType.setName(primaryKeyName);
    primaryKeyType.getColumn().addAll(escapedColumnNames);
    tablesType.getTable().stream().filter(table -> table.getName().equals(tableName)).findFirst().get()
      .setPrimaryKey(primaryKeyType);
  }

  @Override
  public void addForeignKeyForTable(String tableName, String foreignKeyName, String foreignKeyReferencedTable,
    Map<String, String> escapedReferencedColumns) {
    TableType tableType = tablesType.getTable().stream().filter(table -> table.getName().equals(tableName)).findFirst()
      .get();
    if (tableType.getForeignKeys() == null) {
      tableType.setForeignKeys(new ForeignKeysType());
    }
    ForeignKeyType foreignKeyType = new ForeignKeyType();
    foreignKeyType.setName(foreignKeyName);
    foreignKeyType.setReferencedTable(foreignKeyReferencedTable);
    escapedReferencedColumns.forEach((column, ref) -> {
      ReferenceType referenceType = new ReferenceType();
      referenceType.setColumn(column);
      referenceType.setReferenced(ref);
      foreignKeyType.getReference().add(referenceType);
    });
    tableType.getForeignKeys().getForeignKey().add(foreignKeyType);
  }

  @Override
  public void setRowsForTable(String tableName, BigInteger bigInteger) {
    tablesType.getTable().stream().filter(table -> table.getName().equals(tableName)).findFirst().get()
      .setRows(bigInteger);
  }

  @Override
  public void addView(String viewName, String viewQueryOriginal, String viewDescription) {
    ViewType viewType = new ViewType();
    viewType.setName(viewName);
    viewType.setQueryOriginal(viewQueryOriginal);
    viewType.setDescription(viewDescription);
    viewsType.getView().add(viewType);
  }

}
