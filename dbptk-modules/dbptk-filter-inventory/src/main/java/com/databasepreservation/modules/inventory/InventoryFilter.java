/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.inventory;

import java.io.*;
import java.util.*;

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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class InventoryFilter implements DatabaseFilterModule {

  private String prefixPath;
  private String dirPath;
  private final boolean printHeader;
  private char separator;

  private DatabaseFilterModule exportModule;
  private DatabaseStructure databaseStructure;

  private String currentSchemaName;
  private String currentTableName;
  private Map<Integer,String> listOfHeaders;
  private List<Map> listOfData;

  private File csv_file;
  private File csv_dir_path;
  private OutputStreamWriter outStreamWriter;
  private CSVPrinter csv_printer;

  public InventoryFilter(String prefix, String dirPath, boolean pPrintHeader, char separator) {
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
    this.databaseStructure = structure;
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    this.currentSchemaName=schemaName;
    if(!this.dirPath.equals("")){
      this.csv_dir_path = new File(this.dirPath);
      this.csv_dir_path.mkdirs();
    }

    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    TableStructure currentTable = databaseStructure.getTableById(tableId);
    this.currentTableName = currentTable.getId();
    if(!this.dirPath.equals("") && !this.prefixPath.equals("")) {
      this.csv_file = new File(this.csv_dir_path, this.prefixPath + "-" + this.currentTableName + ".csv");
    }else if(!this.dirPath.equals("") && this.prefixPath.equals("")){
      this.csv_file = new File(this.csv_dir_path,  this.currentTableName + ".csv");
    }else if(this.dirPath.equals("") && !this.prefixPath.equals("")){
      this.csv_file = new File(  this.prefixPath + "-" + this.currentTableName + ".csv");
    } else{
      this.csv_file = new File( this.currentTableName + ".csv");
    }

    try {
      this.csv_file.createNewFile();
      this.outStreamWriter = new FileWriter(csv_file);
      this.csv_printer = CSVFormat.newFormat(this.separator).withRecordSeparator("\n").print(this.outStreamWriter);

      this.listOfHeaders = new HashMap<>();
      this.listOfData = new ArrayList<>();

      int index = 1;

      for (ColumnStructure column : currentTable.getColumns()) {
        if (currentTable.isFromCustomView() || ModuleConfigurationManager.getInstance().getModuleConfiguration().isInventoryColumn(currentTable.getSchema(),
                currentTable.getName(), column.getName())) {
          String header = this.currentTableName + "." + column.getName();
          this.listOfHeaders.put(index,header);
          index++;
          if(this.printHeader){
            this.csv_printer.print(header);
          }
        }
      }

      if(this.printHeader){
        this.csv_printer.println();
      }

    } catch (IOException e) {
      throw new ModuleException()
              .withMessage(
                      "Could not create an output stream for file '" + csv_file.toString() + "'")
              .withCause(e);
    }

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {

    Map<Integer,String> lineOfData = new HashMap<>();
    try {
      for (Integer index: this.listOfHeaders.keySet()) {
        Cell cell = row.getCells().get(index);

        if (cell instanceof SimpleCell) {
          SimpleCell simple = ((SimpleCell) cell);
          String data = simple.getSimpleData();
          //String id = cell.getId();
          lineOfData.put(index,data);
          this.csv_printer.print(data);
        }else{
          String data = "N/A";
          lineOfData.put(index,data);
          this.csv_printer.print(data);
        }
      }
      this.csv_printer.println();
      this.listOfData.add(lineOfData);
    } catch (IOException e) {
      throw new ModuleException()
              .withMessage(
                      "Could not write to file '" + csv_file.toString() + "'")
              .withCause(e);
    }

    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {

    try {
      this.outStreamWriter.flush();
      this.outStreamWriter.close();
    } catch (IOException e) {
      throw new ModuleException()
              .withMessage(
                      "Could not close the output stream for file '" + csv_file.toString() + "'")
              .withCause(e);
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
  public void updateModuleConfiguration(String moduleName, Map<String, String> properties, Map<String, String> remoteProperties) {
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