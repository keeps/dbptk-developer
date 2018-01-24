package com.databasepreservation.model.modules.filters;

import java.util.Set;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 * A filter that does not change anything. Its purpose is to provide a sane base
 * class for other filters, so they can change specific parts of the database
 * and leave the remaining parts of the database unchanged.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class IdentityFilter implements DatabaseFilterModule {
  /**
   * Export module (possibly a filter) to call after making the changes specific
   * to this filter
   */
  private DatabaseExportModule exportModule;

  private Reporter reporter;

  public IdentityFilter() {
  }

  @Override
  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule exportModule) throws ModuleException {
    this.exportModule = exportModule;
    return this;
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  /**
   * Method meant to be used by subclasses of IdentityFilter ONLY. And only after
   * the reporter has been set.
   *
   * @return The reported that can be used by classes that inherit IdentityFilter.
   */
  protected Reporter getFilterReporter() {
    return this.reporter;
  }

  @Override
  public ModuleSettings getModuleSettings() throws ModuleException {
    return this.exportModule.getModuleSettings();
  }

  @Override
  public void initDatabase() throws ModuleException {
    this.exportModule.initDatabase();
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    this.exportModule.setIgnoredSchemas(ignoredSchemas);
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    this.exportModule.handleDataCloseTable(tableId);
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    this.exportModule.handleDataCloseSchema(schemaName);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    this.exportModule.finishDatabase();
  }
}
