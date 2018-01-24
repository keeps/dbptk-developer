package com.databasepreservation.common;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Receives notifications about changes in modules
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ModuleObserver {
  void notifyOpenDatabase();

  void notifyStructureObtained(DatabaseStructure structure);

  void notifyOpenSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema);

  void notifyOpenTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema);

  void notifyTableProgressSparse(DatabaseStructure structure, TableStructure table, long completedRows, long totalRows);

  void notifyTableProgressDetailed(DatabaseStructure structure, TableStructure table, Row row, long completedRows,
    long totalRows);

  void notifyCloseTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema);

  void notifyCloseSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema);

  void notifyCloseDatabase(DatabaseStructure structure);
}
