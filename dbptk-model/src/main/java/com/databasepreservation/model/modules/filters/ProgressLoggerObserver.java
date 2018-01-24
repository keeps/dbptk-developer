package com.databasepreservation.model.modules.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.ModuleObserver;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Filter that simply logs the information as it passes through it. In an ideal
 * format for command line output.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ProgressLoggerObserver implements ModuleObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressLoggerObserver.class);

  private boolean showCompletedProgress = false;

  @Override
  public void notifyOpenDatabase() {
    LOGGER.info("Initializing database");
  }

  @Override
  public void notifyStructureObtained(DatabaseStructure structure) {
    LOGGER.info("Obtained database structure");
    LOGGER.info("Exporting database structure");
  }

  @Override
  public void notifyOpenSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema) {
    if (completedSchemas == 0) {
      LOGGER.info("Exporting database structure finished");
    }
    LOGGER.info("Migrating schema '{}' ({} of {} schemas completed)", schema.getName(), completedSchemas,
      structure.getSchemas().size());
  }

  @Override
  public void notifyOpenTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema) {
    LOGGER.info("Migrating table '{}' ({} of {} tables completed)", table.getName(), completedTablesInSchema,
      structure.getSchemaByName(table.getSchema()).getTables().size());
  }

  @Override
  public void notifyTableProgressSparse(DatabaseStructure structure, TableStructure table, long completedRows,
    long totalRows) {
    if (LOGGER.isInfoEnabled() && completedRows > 0) {
      long percent = totalRows > 0 ? completedRows * 100 / totalRows : 0;
      String message = String.format("Progress: %d rows of table '%s' (%d%%)", completedRows, table.getName(), percent);
      LOGGER.info(message);
      showCompletedProgress = true;
    }
  }

  @Override
  public void notifyTableProgressDetailed(DatabaseStructure structure, TableStructure table, Row row,
    long completedRows, long totalRows) {
    // no-op. There is no need for this much detail in this observer
  }

  @Override
  public void notifyCloseTable(DatabaseStructure structure, TableStructure table, long completedSchemas,
    long completedTablesInSchema) {
    if (showCompletedProgress) {
      // if progress percent has been shown before, show progress at 100%
      notifyTableProgressSparse(structure, table, table.getRows(), table.getRows());
      showCompletedProgress = false;
    }

    LOGGER.info("Completed table '{}' ({} of {} tables completed)", table.getName(), completedTablesInSchema,
      structure.getSchemaByName(table.getSchema()).getTables().size());
  }

  @Override
  public void notifyCloseSchema(DatabaseStructure structure, SchemaStructure schema, long completedSchemas,
    long completedTablesInSchema) {
    LOGGER.info("Completed schema '{}' ({} of {} schemas completed)", schema.getName(), completedSchemas,
      structure.getSchemas().size());
  }

  @Override
  public void notifyCloseDatabase(DatabaseStructure structure) {
    LOGGER.info("Completed database migration");
  }
}
