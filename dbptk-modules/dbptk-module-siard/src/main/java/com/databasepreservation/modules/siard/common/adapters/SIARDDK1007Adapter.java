package com.databasepreservation.modules.siard.common.adapters;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.modules.siard.bindings.siard_dk_1007.ArchiveIndex;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQLStandardDatatypeImporter;
import dk.sa.xmlns.diark._1_0.docindex.DocIndexType;
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
import org.apache.commons.lang3.StringUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class SIARDDK1007Adapter implements SIARDDKAdapter {
  private SiardDiark siardDiark;
  private ArchiveIndex archiveIndex;
  private TablesType tablesType;
  private TableType tableType;
  private ViewsType viewsType;

  public SIARDDK1007Adapter() {
    siardDiark = new SiardDiark();
    tablesType = new TablesType();

    siardDiark.setTables(tablesType);
  }

  @Override
  public void setSiardDiark(Object siardDiark) {
    this.siardDiark = (SiardDiark) siardDiark;
  }

  @Override
  public void setArchiveIndex(Object archiveIndex) {
    this.archiveIndex = (ArchiveIndex) archiveIndex;
  }

  @Override
  public void setTableType(Object tableType) {
    this.tableType = (TableType) tableType;
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
  public PrimaryKey getPrimaryKey(Object primaryKeyXml) {
    PrimaryKey keyDptkl = new PrimaryKey();
    PrimaryKeyType primaryKeyType = (PrimaryKeyType) primaryKeyXml;
    keyDptkl.setName(primaryKeyType.getName());
    keyDptkl.setColumnNames(primaryKeyType.getColumn());
    return keyDptkl;
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
  public List<ForeignKey> getForeignKeys(Object foreignKeysXml, String tableId, String importAsSchemaName) {
    List<ForeignKey> lstForeignKeyDptkl = new LinkedList<ForeignKey>();
    ForeignKeysType foreignKeysType = (ForeignKeysType) foreignKeysXml;
    if (foreignKeysXml != null) {
      for (ForeignKeyType foreignKeyXml : foreignKeysType.getForeignKey()) {
        ForeignKey foreignKeyDptkl = new ForeignKey();
        foreignKeyDptkl.setReferencedSchema(importAsSchemaName);
        foreignKeyDptkl.setName(foreignKeyXml.getName());
        foreignKeyDptkl.setReferencedTable(foreignKeyXml.getReferencedTable());
        foreignKeyDptkl.setReferences(getReferences(foreignKeyXml.getReference()));
        foreignKeyDptkl.setId(String.format("%s.%s", tableId, foreignKeyDptkl.getName()));
        lstForeignKeyDptkl.add(foreignKeyDptkl);
      }
    }
    return lstForeignKeyDptkl;
  }

  @Override
  public List<ColumnStructure> getTblColumns(Object columnsXml, String tableId,
    SQLStandardDatatypeImporter sqlStandardDatatypeImporter) {
    List<ColumnStructure> lstColumnsDptkl = new LinkedList<ColumnStructure>();
    ColumnsType columnsType = (ColumnsType) columnsXml;
    if (columnsType != null && columnsType.getColumn() != null) {
      for (ColumnType columnXml : columnsType.getColumn()) {
        ColumnStructure columnDptkl = new ColumnStructure();
        columnDptkl.setName(columnXml.getName());
        columnDptkl.setId(String.format("%s.%s", tableId, columnDptkl.getName()));
        String typeOriginal = StringUtils.isNotBlank(columnXml.getTypeOriginal()) ? columnXml.getTypeOriginal() : null;
        columnDptkl
          .setType(sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
            "<information unavailable>", "<information unavailable>", columnXml.getType(), typeOriginal));
        columnDptkl.setDescription(columnXml.getDescription());
        String defaultValue = StringUtils.isNotBlank(columnXml.getDefaultValue()) ? columnXml.getDefaultValue() : null;
        columnDptkl.setDefaultValue(defaultValue);
        columnDptkl.setNillable(columnXml.isNullable());
        lstColumnsDptkl.add(columnDptkl);
      }
    }
    return lstColumnsDptkl;
  }

  @Override
  public List<ForeignKey> getVirtualForeignKeys(Object columns, String tableId, String importAsSchemaName) {
    List<ForeignKey> virtualForeignKeys = new ArrayList<>();
    ColumnsType columnsType = (ColumnsType) columns;
    for (ColumnType column : columnsType.getColumn()) {
      if (column.getFunctionalDescription() != null
              && column.getFunctionalDescription().contains(FunctionalDescriptionType.DOKUMENTIDENTIFIKATION)) {
        ForeignKey virtualForeignKey = new ForeignKey();
        virtualForeignKey.setReferencedSchema(importAsSchemaName);
        virtualForeignKey.setName("FK_virtual_table");
        virtualForeignKey.setReferencedTable("virtual_table");
        Reference reference = new Reference(column.getName(), "dID");
        List<Reference> referenceList = new ArrayList<>();
        referenceList.add(reference);
        virtualForeignKey.setReferences(referenceList);
        virtualForeignKey.setId(String.format("%s.%s", tableId, "FK_virtual_table"));
        virtualForeignKeys.add(virtualForeignKey);
      }
    }

    return virtualForeignKeys;
  }

  protected List<Reference> getReferences(List<ReferenceType> referencesXml) {
    List<Reference> refsDptkld = new LinkedList<Reference>();
    if (referencesXml != null) {
      for (ReferenceType referenceTypeXml : referencesXml) {
        Reference refDptkld = new Reference();
        refDptkld.setColumn(referenceTypeXml.getColumn());
        refDptkld.setReferenced(referenceTypeXml.getReferenced());
        refsDptkld.add(refDptkld);
      }
    }
    return refsDptkld;
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
  public String getDocIndexTypePackageName() {
    return DocIndexType.class.getPackage().getName();
  }

  @Override
  public String getArchiveIndexPackageName() {
    return ArchiveIndex.class.getPackage().getName();
  }

}
