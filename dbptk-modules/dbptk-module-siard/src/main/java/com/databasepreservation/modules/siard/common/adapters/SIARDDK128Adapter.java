package com.databasepreservation.modules.siard.common.adapters;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ArchiveIndex;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ColumnType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ColumnsType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ForeignKeyType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ForeignKeysType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.FunctionalDescriptionType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.PrimaryKeyType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ReferenceType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.SiardDiark;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.TableType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.TablesType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ViewType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ViewsType;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class SIARDDK128Adapter implements SIARDDKAdapter {
  private SiardDiark siardDiark;
  private ArchiveIndex archiveIndex;
  private final TablesType tablesType;
  private TableType tableType;
  private ViewsType viewsType;

  public SIARDDK128Adapter() {
    siardDiark = new SiardDiark();
    tablesType = new TablesType();

    siardDiark.setTables(tablesType);
  }

  @Override
  public void setSiardDiark(Object siardDiark) {
    this.siardDiark = (SiardDiark) siardDiark;
  }

  @Override
  public void setPrimaryKey(Object primaryKey) {
    this.primaryKey = (PrimaryKey) primaryKey;
  }

  @Override
  public void setArchiveIndex(Object archiveIndex) {
    this.archiveIndex = (ArchiveIndex) archiveIndex;
  }

  @Override
  public void setVersion(String value) {
    siardDiark.setVersion(value);
  }

  @Override
  public void setTableType(Object tableType) {
    this.tableType = (TableType) tableType;
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
  public String getDatabaseProduct() {
    return siardDiark.getDatabaseProduct();
  }

  @Override
  public String getDbName() {
    return siardDiark.getDbName();
  }

  @Override
  public Object getTables() {
    return siardDiark.getTables();
  }

  @Override
  public String getTableTypeName() {
    return tableType.getName();
  }

  @Override
  public String getTableTypeFolder() {
    return tableType.getFolder();
  }

  @Override
  public String getTableTypeDescription() {
    return tableType.getDescription();
  }

  @Override
  public Object getTableTypePrimaryKey() {
    return tableType.getPrimaryKey();
  }

  @Override
  public Object getTableTypeForeignKeys() {
    return tableType.getForeignKeys();
  }

  @Override
  public BigInteger getTableTypeRows() {
    return tableType.getRows();
  }

  @Override
  public Object getTableTypeColumns() {
    return tableType.getColumns();
  }

  @Override
  public PrimaryKey getPrimaryKey(Object primaryKeyXml) {
    PrimaryKey keyDptkl = new PrimaryKey();
    PrimaryKeyType primaryKeyType = (PrimaryKeyType) primaryKeyXml;
    keyDptkl.setName(primaryKeyType.getName());
    keyDptkl.setColumnNames(primaryKeyType.getColumn());
    return keyDptkl;
  }

  @Override
  public List<Object> getTable() {
    return Collections.singletonList(siardDiark.getTables().getTable());
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
    if (viewsType == null) {
      viewsType = new ViewsType();
      siardDiark.setViews(viewsType);
    }
    ViewType viewType = new ViewType();
    viewType.setName(viewName);
    viewType.setQueryOriginal(viewQueryOriginal);
    viewType.setDescription(viewDescription);
    viewsType.getView().add(viewType);
  }

  @Override
  public String getSiardDiarkPackageName() {
    return SiardDiark.class.getPackage().getName();
  }

  @Override
  public String getArchiveIndexPackageName() {
    return "";
  }

}
