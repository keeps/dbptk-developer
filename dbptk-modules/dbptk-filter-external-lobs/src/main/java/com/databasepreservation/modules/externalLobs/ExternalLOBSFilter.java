/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;

public class ExternalLOBSFilter implements DatabaseFilterModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSFilter.class);

  private DatabaseExportModule exportModule;
  private Reporter reporter;
  private Map<String, Map<String, List<String>>> externalLOBS;
  private Map<String, List<String>> currentSchema;
  private ExternalLOBSCellHandler cellHandler;

  private DatabaseStructure databaseStructure;
  private boolean hasExternalLOBS = false;
  private List<Integer> externalLOBIndexes = new ArrayList<>();

  public ExternalLOBSFilter(Map<String, Map<String, List<String>>> externalLOBS, ExternalLOBSCellHandler cellHandler) {
    this.externalLOBS = externalLOBS;
    this.cellHandler = cellHandler;
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
    for (String schema : externalLOBS.keySet()) {
      SchemaStructure schemaStructure = structure.getSchemaByName(schema);
      if (schemaStructure != null) {
        for (String table : externalLOBS.get(schema).keySet()) {
          List<String> columns = externalLOBS.get(schema).get(table);
          try {
            TableStructure tableStructure = schemaStructure.getTableById(schema + "." + table);
            if (tableStructure != null) {
              for (ColumnStructure column : tableStructure.getColumns()) {
                if (columns.contains(column.getName())) {
                  // todo: handle both BLOBS and CLOBS (possibly through params)
                  Type original = column.getType();
                  Type newType = new SimpleTypeBinary();
                  newType.setDescription(cellHandler.handleTypeDescription(original.getDescription()));
                  newType.setSql99TypeName("BINARY VARYING", 1);
                  newType.setSql2008TypeName("BINARY VARYING", 1);
                  newType.setOriginalTypeName(original.getOriginalTypeName());
                  ((SimpleTypeBinary) newType).setOutsideDatabase(true);

                  column.setType(newType);
                }
              }
            } else {
              LOGGER.warn("Table {}, referenced in column list file, was not found in schema {}", table, schema);
            }
          } catch (Exception e) {
            throw new ModuleException().withMessage("Error setting column type in structure").withCause(e);
          }
        }
      } else {
        LOGGER.warn("Schema {}, referenced in column list file, was not found in the database", schema);
      }
    }

    this.databaseStructure = structure;
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    currentSchema = externalLOBS.get(schemaName);
    if (currentSchema == null) {
      throw new ModuleException().withMessage("Unrecognized schema name " + schemaName);
    }
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    List<ColumnStructure> columns = databaseStructure.getTableById(tableId).getColumns();
    String tableName = databaseStructure.getTableById(tableId).getName();
    if (currentSchema.keySet().contains(tableName)) {
      hasExternalLOBS = true;
      List<String> cellList = currentSchema.get(tableName);

      // check which column indexes are paths to external lobs
      for (int i = 0; i < columns.size(); i++) {
        if (cellList.contains(columns.get(i).getName())) {
          externalLOBIndexes.add(i);
        }
      }
    } else {
      hasExternalLOBS = false;
    }
    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    if (hasExternalLOBS) {
      List<Cell> rowCells = row.getCells();
      for (int index : externalLOBIndexes) {
        Cell cell = rowCells.get(index);

        if(cell instanceof SimpleCell){
          String reference = ((SimpleCell) cell).getSimpleData();
          if (reference != null && !reference.isEmpty()) {
            Cell newCell = cellHandler.handleCell(cell.getId(), ((SimpleCell) cell).getSimpleData());
            rowCells.set(index, newCell);
          } else {
            reporter.ignored("Cell " + cell.getId(), "reference to external LOB is null");
            rowCells.set(index, new NullCell(cell.getId()));
          }
        } else {
          LOGGER.error("Reference to LOB is not a SimpleCell");
          rowCells.set(index, new NullCell(cell.getId()));
        }

      }
      row.setCells(rowCells);
    }
    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    hasExternalLOBS = false;
    externalLOBIndexes = new ArrayList<>();
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

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }
}
