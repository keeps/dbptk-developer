/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.csv;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.utils.MessageDigestUtils;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import sun.util.resources.cldr.zh.CalendarData_zh_Hans_HK;

public class CsvFileFilter implements DatabaseFilterModule {
  private static final String MERKLE_FIELD_NAME = "merkle";
  private static final String TOP_HASH_FIELD_NAME = "topHash";
  private static final String ALGORITHM_FIELD_NAME = "algorithm";
  private static final String SCHEMAS_FIELD_NAME = "schemas";
  private static final String TABLES_FIELD_NAME = "tables";
  private static final String COLUMNS_FIELD_NAME = "columns";
  private static final String ROWS_FIELD_NAME = "rows";
  private static final String CELLS_FIELD_NAME = "cells";
  private static final String INDEX_FIELD_NAME = "index";
  private static final String SCHEMA_HASH_FIELD_NAME = "schemaHash";
  private static final String TABLE_HASH_FIELD_NAME = "tableHash";
  private static final String ROW_HASH_FIELD_NAME = "rowHash";
  private static final String CELL_HASH_FIELD_NAME = "cellHash";
  public static final String UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE = "Unable to write to the output file";

  private Path outputFile;
  private final boolean printHeader;
  private String separator;

  private DatabaseFilterModule exportModule;
  private DatabaseStructure databaseStructure;
  private OutputStream outputStream;

  private String currentSchemaName;
  private String currentTableName;
  private Map<String,Map> listOfHeaders = new HashMap<>();;
  private Map<String,List> listOfData = new HashMap<>();


  private int numberHeaders = 0;
  private List<String> finalHeaders = new ArrayList<>();

  public CsvFileFilter(Path outputFile, boolean pPrintHeader, String separator) {
    this.outputFile = outputFile;
    this.printHeader = pPrintHeader;
    this.separator = separator;
  }

  @Override
  public void initDatabase() throws ModuleException {
    try {
      outputStream = new BufferedOutputStream(new FileOutputStream(outputFile.toFile()));



    } catch (IOException e) {
      throw new ModuleException()
        .withMessage(
          "Could not create an output stream for file '" + outputFile.normalize().toAbsolutePath().toString() + "'")
        .withCause(e);
    }

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
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {

    List<Map> linesOfData = new ArrayList<>();
    Map<Integer,String> columnsHeader = new HashMap<>();

    TableStructure currentTable = databaseStructure.getTableById(tableId);
    this.currentTableName = currentTable.getId();

    int index = 0;

    for (ColumnStructure column : currentTable.getColumns()) {
      if (currentTable.isFromCustomView() || ModuleConfigurationManager.getInstance().getModuleConfiguration().isMerkleColumn(currentTable.getSchema(),
        currentTable.getName(), column.getName())) {
        columnsHeader.put(index,column.getName());
        index++;
      }
    }

    this.listOfHeaders.put(this.currentTableName,columnsHeader);
    this.listOfData.put(this.currentTableName,linesOfData);

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {

    Map<Integer,String> lineData = new HashMap<>();

    List<Map> linesToAppend = this.listOfData.get(this.currentTableName);

    Map<Integer,String> columns = this.listOfHeaders.get(this.currentTableName);

    for (Integer index: columns.keySet()) {
      Cell cell = row.getCells().get(index);

      if (cell instanceof SimpleCell) {
        SimpleCell simple = ((SimpleCell) cell);
        String data = simple.getSimpleData();
        //String id = cell.getId();
        lineData.put(index,data);
      }

    }

    linesToAppend.add(lineData);
    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    this.numberHeaders+=this.listOfHeaders.get(this.currentTableName).size();
    Map tableHeaders = (HashMap) this.listOfHeaders.get(this.currentTableName);
    for(Object pos: tableHeaders.keySet()){
      String header = this.currentTableName + "." + tableHeaders.get(pos).toString();
      this.finalHeaders.add(header);
    }

    //organizar aqui a data

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