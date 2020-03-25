/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules;

import java.util.Map;
import java.util.Set;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SinkModule implements DatabaseFilterModule {

  public SinkModule() {
    super();
  }

  @Override
  public void initDatabase() {
    // do nothing
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    // do nothing
  }

  @Override
  public void handleStructure(DatabaseStructure structure) {
    // do nothing
  }

  @Override
  public void handleDataOpenSchema(String schemaName) {
    // do nothing
  }

  @Override
  public void handleDataOpenTable(String tableId) {
    // do nothing
  }

  @Override
  public void handleDataRow(Row row) {
    for (Cell cell : row.getCells()) {
      if (cell instanceof BinaryCell) {
        ((BinaryCell) cell).cleanResources();
      }
    }
  }

  @Override
  public void handleDataCloseTable(String tableId) {
    // do nothing
  }

  @Override
  public void handleDataCloseSchema(String schemaName) {
    // do nothing
  }

  @Override
  public void finishDatabase() {
    // do nothing
  }

  @Override
  public void updateModuleConfiguration(String moduleName, Map<String, String> properties,
    Map<String, String> remoteProperties) {
    // do nothing
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    // do nothing
  }

  @Override
  public DatabaseFilterModule migrateDatabaseTo(DatabaseFilterModule databaseExportModule) {
    return this;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }
}
