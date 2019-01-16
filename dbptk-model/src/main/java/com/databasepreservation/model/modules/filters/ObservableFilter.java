/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.filters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.databasepreservation.common.ModuleObserver;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Filter that notifies observers of specific conversion events (like starting
 * to convert a table, converting a row, etc).
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ObservableFilter extends IdentityFilter {
  private static final int SPARSE_PROGRESS_MINIMUM_TIME = 3000;
  private static final int SPARSE_PROGRESS_MINIMUM_ROWS = 1000;

  private final List<ModuleObserver> observers;

  private DatabaseStructure structure = null;
  private SchemaStructure schema = null;
  private TableStructure table = null;
  private long completedRows = 0;
  private long lastSparseProgressTimestamp = 0;

  public ObservableFilter() {
    super();
    this.observers = new ArrayList<>();
  }

  public ObservableFilter(final ModuleObserver... observers) {
    this();
    Collections.addAll(this.observers, observers);
  }

  public boolean addModuleObserver(ModuleObserver observer) {
    return observers.add(observer);
  }

  public boolean removeModuleObserver(ModuleObserver observer) {
    return observers.remove(observer);
  }

  @Override
  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule exportModule) throws ModuleException {
    for (ModuleObserver observer : observers) {
      observer.notifyOpenDatabase();
    }
    return super.migrateDatabaseTo(exportModule);
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    super.setIgnoredSchemas(ignoredSchemas);
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    this.structure = structure;
    for (ModuleObserver observer : observers) {
      observer.notifyStructureObtained(structure);
    }
    super.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    schema = structure.getSchemaByName(schemaName);
    int completedSchemas = schema.getIndex() - 1;
    int completedTables = 0;
    for (ModuleObserver observer : observers) {
      observer.notifyOpenSchema(structure, schema, completedSchemas, completedTables);
    }
    super.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    table = schema.getTableById(tableId);
    long completedSchemas = schema.getIndex();
    long completedTables = table.getIndex() - 1;

    for (ModuleObserver observer : observers) {
      observer.notifyOpenTable(structure, table, completedSchemas, completedTables);
    }
    super.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    long totalRows = table.getRows();

    // notify detailed observers
    for (ModuleObserver observer : observers) {
      observer.notifyTableProgressDetailed(structure, table, row, completedRows, totalRows);
    }

    // notify sparse observers
    if (completedRows % SPARSE_PROGRESS_MINIMUM_ROWS == 0
      && System.currentTimeMillis() - lastSparseProgressTimestamp > SPARSE_PROGRESS_MINIMUM_TIME) {
      lastSparseProgressTimestamp = System.currentTimeMillis();
      for (ModuleObserver observer : observers) {
        observer.notifyTableProgressSparse(structure, table, completedRows, totalRows);
      }
    }

    completedRows++;
    super.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    super.handleDataCloseTable(tableId);

    completedRows = 0;
    int completedSchemas = schema.getIndex();
    int completedTables = table.getIndex();
    for (ModuleObserver observer : observers) {
      observer.notifyCloseTable(structure, table, completedSchemas, completedTables);
    }
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    super.handleDataCloseSchema(schemaName);

    int completedSchemas = schema.getIndex();
    int completedTables = table.getIndex();
    for (ModuleObserver observer : observers) {
      observer.notifyCloseSchema(structure, schema, completedSchemas, completedTables);
    }
  }

  @Override
  public void finishDatabase() throws ModuleException {
    super.finishDatabase();

    for (ModuleObserver observer : observers) {
      observer.notifyCloseDatabase(structure);
    }
  }
}
