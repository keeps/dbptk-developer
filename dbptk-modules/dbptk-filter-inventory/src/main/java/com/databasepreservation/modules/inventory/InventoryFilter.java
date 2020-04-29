/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.inventory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.TableStructure;

public class InventoryFilter implements DatabaseFilterModule {

  private DatabaseFilterModule exportModule;
  private DatabaseStructure databaseStructure;

  private List<Integer> indexOfHeaders;

  private String prefixPath;
  private Path dirPath;
  private final boolean printHeader;
  private String separator;

  private File csv_file;
  private OutputStreamWriter outStreamWriter;
  private CSVPrinter csv_printer;
  private boolean do_export;

  public InventoryFilter(String prefix, Path dirPath, boolean pPrintHeader, String separator) {
    this.prefixPath = prefix;
    this.dirPath = dirPath;
    this.printHeader = pPrintHeader;
    this.separator = separator;
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
    databaseStructure = structure;
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    do_export = false;
    TableStructure currentTable = databaseStructure.getTableById(tableId);
    String currentTableName = currentTable.getId();

    indexOfHeaders = new ArrayList<>();
    List<String> listOfHeaders = new ArrayList<>();

    try {
      int index = 0;

      for (ColumnStructure column : currentTable.getColumns()) {
        if (currentTable.isFromCustomView() || ModuleConfigurationManager.getInstance().getModuleConfiguration()
          .isInventoryColumn(currentTable.getSchema(), currentTable.getName(), column.getName())) {
          String header = column.getName();
          do_export = true;
          indexOfHeaders.add(index);
          listOfHeaders.add(header);
        }
        index++;
      }

      if (do_export) {
        csv_file = dirPath.resolve(prefixPath + currentTableName + ".csv").toFile();
        outStreamWriter = new FileWriter(csv_file);
        csv_printer = CSVFormat.newFormat(separator.charAt(0)).withRecordSeparator("\n").print(outStreamWriter);
        if (printHeader) {
          for (String header : listOfHeaders) {
            csv_printer.print(header);
          }
          csv_printer.println();
        }
      }
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not create an output stream for file '" + csv_file.toString() + "'").withCause(e);
    }

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    if (do_export) {
      try {
        for (Integer index : indexOfHeaders) {
          Cell cell = row.getCells().get(index);

          if (cell instanceof SimpleCell) {
            SimpleCell simple = ((SimpleCell) cell);
            String data = simple.getSimpleData();
            csv_printer.print(data);
          } else {
            String data = "N/A";
            csv_printer.print(data);
          }
        }
        csv_printer.println();
      } catch (IOException e) {
        throw new ModuleException().withMessage("Could not write to file '" + csv_file.toString() + "'").withCause(e);
      }
    }
    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    if (do_export) {
      try {
        outStreamWriter.flush();
        outStreamWriter.close();
      } catch (IOException e) {
        throw new ModuleException()
          .withMessage("Could not close the output stream for file '" + csv_file.toString() + "'").withCause(e);
      }
    }
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
    this.exportModule = databaseExportModule;
    return this;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }

}