/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;

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
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK1007TableIndexFileStrategy extends
  SIARDDKTableIndexFileStrategy<SiardDiark, TablesType, TableType, ColumnType, ColumnsType, PrimaryKeyType, ForeignKeysType, ForeignKeyType, ReferenceType, ViewType, ViewsType, FunctionalDescriptionType> {
  public SIARDDK1007TableIndexFileStrategy(LOBsTracker lobsTracker) {
    super(lobsTracker);
  }

  @Override
  SiardDiark createSiardDiarkInstance() {
    return new SiardDiark();
  }

  @Override
  TableType createTableTypeInstance() {
    return new TableType();
  }

  @Override
  TablesType createTablesTypeInstance() {
    return new TablesType();
  }

  @Override
  ColumnType createColumnTypeInstance() {
    return new ColumnType();
  }

  @Override
  ColumnsType createColumnsTypeInstance() {
    return new ColumnsType();
  }

  @Override
  PrimaryKeyType createPrimaryKeyTypeInstance() {
    return new PrimaryKeyType();
  }

  @Override
  ForeignKeysType createForeignKeysTypeInstance() {
    return new ForeignKeysType();
  }

  @Override
  ForeignKeyType createForeignKeyTypeInstance() {
    return new ForeignKeyType();
  }

  @Override
  ReferenceType createReferenceTypeInstance() {
    return new ReferenceType();
  }

  @Override
  ViewType createViewTypeInstance() {
    return new ViewType();
  }

  @Override
  ViewsType createViewsTypeInstance() {
    return new ViewsType();
  }

  @Override
  void setDbName(SiardDiark siardDiark, String dbName) {
    siardDiark.setDbName(dbName);
  }

  @Override
  void setVersion(SiardDiark siardDiark, String version) {
    siardDiark.setVersion(version);
  }

  @Override
  void setDatabaseProduct(SiardDiark siardDiark, String databaseProduct) {
    siardDiark.setDatabaseProduct(databaseProduct);
  }

  @Override
  void setColumnName(ColumnType columnType, String name) {
    columnType.setName(name);
  }

  @Override
  void setColumnID(ColumnType columnType, String id) {
    columnType.setColumnID(id);
  }

  @Override
  void setType(ColumnType columnType, String type) {
    columnType.setType(type);
  }

  @Override
  void setTypeOriginal(ColumnType columnType, String typeOriginal) {
    columnType.setTypeOriginal(typeOriginal);
  }

  @Override
  void setDefaultValue(ColumnType columnType, String defaultValue) {
    columnType.setDefaultValue(defaultValue);
  }

  @Override
  void setNullable(ColumnType columnType, boolean nullable) {
    columnType.setNullable(nullable);
  }

  @Override
  List<FunctionalDescriptionType> getFunctionalDescription(ColumnType columnType) {
    return columnType.getFunctionalDescription();
  }

  @Override
  void setColumnDescription(ColumnType columnType, String description) {
    columnType.setDescription(description);
  }

  @Override
  void setTableName(TableType tableType, String name) {
    tableType.setName(name);
  }

  @Override
  void setFolder(TableType tableType, String folder) {
    tableType.setFolder(folder);
  }

  @Override
  void setTableDescription(TableType tableType, String description) {
    tableType.setDescription(description);
  }

  @Override
  void setColumns(TableType tableType, ColumnsType columns) {
    tableType.setColumns(columns);
  }

  @Override
  void setForeignKeys(TableType tableType, ForeignKeysType foreignKeysType) {
    tableType.setForeignKeys(foreignKeysType);
  }

  @Override
  void setRows(TableType tableType, BigInteger rows) {
    tableType.setRows(rows);
  }

  @Override
  void setPrimaryKey(TableType tableType, PrimaryKeyType primaryKeyType) {
    tableType.setPrimaryKey(primaryKeyType);
  }

  @Override
  List<TableType> getTable(TablesType tablesType) {
    return tablesType.getTable();
  }

  @Override
  List<ColumnType> getColumn(ColumnsType columns) {
    return columns.getColumn();
  }

  @Override
  FunctionalDescriptionType createDOKUMENTIDENTIFIKATION() {
    return FunctionalDescriptionType.DOKUMENTIDENTIFIKATION;
  }

  @Override
  void setPrimaryKeyName(PrimaryKeyType primaryKeyType, String name) {
    primaryKeyType.setName(name);
  }

  @Override
  List<String> getPrimaryKeyTypeColumn(PrimaryKeyType primaryKeyType) {
    return primaryKeyType.getColumn();
  }

  @Override
  void setForeignKeyName(ForeignKeyType foreignKeyType, String name) {
    foreignKeyType.setName(name);
  }

  @Override
  List<ReferenceType> getReference(ForeignKeyType foreignKeyType) {
    return foreignKeyType.getReference();
  }

  @Override
  List<ForeignKeyType> getForeignKey(ForeignKeysType foreignKeysType) {
    return foreignKeysType.getForeignKey();
  }

  @Override
  void setReferencedTable(ForeignKeyType foreignKeyType, String referencedTable) {
    foreignKeyType.setReferencedTable(referencedTable);
  }

  @Override
  void setReferencedColumn(ReferenceType referenceType, String referencedColumn) {
    referenceType.setColumn(referencedColumn);
  }

  @Override
  void setReferenced(ReferenceType referenceType, String referenced) {
    referenceType.setReferenced(referenced);
  }

  @Override
  void setViewName(ViewType viewType, String name) {
    viewType.setName(name);
  }

  @Override
  void setQueryOriginal(ViewType viewType, String query) {
    viewType.setQueryOriginal(query);
  }

  @Override
  void setViewDescription(ViewType viewType, String description) {
    viewType.setDescription(description);
  }

  @Override
  List<ViewType> getView(ViewsType viewsType) {
    return viewsType.getView();
  }

  @Override
  void setViews(SiardDiark siardDiark, ViewsType viewsType) {
    siardDiark.setViews(viewsType);
  }

  @Override
  void setTables(SiardDiark siardDiark, TablesType tablesType) {
    siardDiark.setTables(tablesType);
  }
}
