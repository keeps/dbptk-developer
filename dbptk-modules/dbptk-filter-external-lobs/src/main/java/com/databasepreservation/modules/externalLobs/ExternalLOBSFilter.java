package com.databasepreservation.modules.externalLobs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;

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
      for (String table : externalLOBS.get(schema).keySet()) {
        List<String> columns = externalLOBS.get(schema).get(table);
        try {
          for (ColumnStructure column : structure.getSchemaByName(schema).getTableById(schema + "." + table).getColumns()) {
            if (columns.contains(column.getName())) {
              // todo: handle both BLOBS and CLOBS (possibly through params)
              Type original = column.getType();
              Type newType = new SimpleTypeBinary();
              newType.setDescription(cellHandler.handleTypeDescription(original.getDescription()));
              newType.setSql99TypeName("BINARY VARYING",1);
              newType.setSql2008TypeName("BINARY VARYING",1);
              newType.setOriginalTypeName(original.getOriginalTypeName());
              ((SimpleTypeBinary) newType).setOutsideDatabase(true);

              column.setType(newType);
            }
          }
        } catch (Exception e) {
          throw new ModuleException().withMessage("Error setting column type in structure").withCause(e);
        }
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
          BinaryCell newCell = cellHandler.handleCell(cell.getId() ,((SimpleCell) cell).getSimpleData());
          rowCells.set(index, newCell);
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
