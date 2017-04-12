package com.databasepreservation.common;

import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Updates ModuleObservers about conversion events (e.g. progress)
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class ObservableModule {
  private final List<ModuleObserver> observers;

  public ObservableModule() {
    super();
    this.observers = new ArrayList<>();
  }

  public void addModuleObserver(ModuleObserver observer) {
    observers.add(observer);
  }

  public void removeModuleObserver(ModuleObserver observer) {
    observers.remove(observer);
  }

  /**************************************************************************
   * Events notification
   */

  public void notifyOpenDatabase() {
    for (ModuleObserver observer : observers) {
      observer.notifyOpenDatabase();
    }
  }

  public void notifyStructureObtained(DatabaseStructure structure) {
    for (ModuleObserver observer : observers) {
      observer.notifyStructureObtained(structure);
    }
  }

  public void notifyOpenSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema) {
    for (ModuleObserver observer : observers) {
      observer.notifyOpenSchema(structure, schema, completedSchemas, completedTablesInSchema);
    }
  }

  public void notifyOpenTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema) {
    for (ModuleObserver observer : observers) {
      observer.notifyOpenTable(structure, table, completedSchemas, completedTablesInSchema);
    }
  }

  /**
   * Notify about progress in converting a table. Delta between 2 reports may be
   * more than a single row.
   */
  public void notifyTableProgress(DatabaseStructure structure, TableStructure table, long completedRows, long totalRows) {
    for (ModuleObserver observer : observers) {
      observer.notifyTableProgress(structure, table, completedRows, totalRows);
    }
  }

  public void notifyCloseTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema) {
    for (ModuleObserver observer : observers) {
      observer.notifyCloseTable(structure, table, completedSchemas, completedTablesInSchema);
    }
  }

  public void notifyCloseSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema) {
    for (ModuleObserver observer : observers) {
      observer.notifyCloseSchema(structure, schema, completedSchemas, completedTablesInSchema);
    }
  }

  public void notifyCloseDatabase(DatabaseStructure structure) {
    for (ModuleObserver observer : observers) {
      observer.notifyCloseDatabase(structure);
    }
  }

}
