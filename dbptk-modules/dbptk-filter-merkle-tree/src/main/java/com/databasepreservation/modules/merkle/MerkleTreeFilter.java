/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.merkle;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class MerkleTreeFilter implements DatabaseFilterModule {
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

  private DatabaseFilterModule exportModule;
  private DatabaseStructure databaseStructure;
  private OutputStream outputStream;

  private Path outputFile;
  private String messageDigestAlgorithm;
  private final boolean explain;
  private final boolean lowerCase;

  private MessageDigest databaseDigest;
  private MessageDigest schemaDigest;
  private MessageDigest tableDigest;

  private List<Integer> merkleColumnsIndexes = new ArrayList<>();

  private JsonGenerator jsonGenerator;

  public MerkleTreeFilter(Path outputFile, String messageDigestAlgorithm, boolean explain, String pFontCase) {
    this.outputFile = outputFile;
    this.messageDigestAlgorithm = messageDigestAlgorithm;
    this.lowerCase = pFontCase.equalsIgnoreCase("lowercase");
    this.explain = explain;
  }

  @Override
  public void initDatabase() throws ModuleException {
    try {
      outputStream = new BufferedOutputStream(new FileOutputStream(outputFile.toFile()));
      JsonFactory jsonFactory = new JsonFactory();
      jsonGenerator = jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(MERKLE_FIELD_NAME);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeObjectField(ALGORITHM_FIELD_NAME, messageDigestAlgorithm.toUpperCase());
      databaseDigest = getMessageDigest();
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
    try {
      jsonGenerator.writeFieldName(SCHEMAS_FIELD_NAME);
      jsonGenerator.writeStartArray();
    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    this.schemaDigest = getMessageDigest();
    try {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(schemaName);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(TABLES_FIELD_NAME);
      jsonGenerator.writeStartArray();
    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    this.tableDigest = getMessageDigest();
    TableStructure currentTable = databaseStructure.getTableById(tableId);

    int index = 0;

    List<String> columns = new ArrayList<>();

    for (ColumnStructure column : currentTable.getColumns()) {
      if (currentTable.isFromCustomView() || ModuleConfigurationManager.getInstance().getModuleConfiguration().isMerkleColumn(currentTable.getSchema(), currentTable.getName(), column.getName())) {
        merkleColumnsIndexes.add(index);
        columns.add(column.getName());
      }
      index++;
    }

    try {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(currentTable.getName());
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName(COLUMNS_FIELD_NAME);
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue(jsonGenerator, columns);

      if (explain) {
        jsonGenerator.writeFieldName(ROWS_FIELD_NAME);
        jsonGenerator.writeStartArray();
      }

    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    try {
      MessageDigest rowDigest = getMessageDigest();
      if (explain) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField(INDEX_FIELD_NAME, row.getIndex());
        jsonGenerator.writeFieldName(CELLS_FIELD_NAME);
        jsonGenerator.writeStartArray();
      }

      for (int i = 0; i < merkleColumnsIndexes.size(); i++) {
        Cell cell = row.getCells().get(merkleColumnsIndexes.get(i));
        byte[] digest;
        MessageDigest cellDigest = getMessageDigest();

        if (cell instanceof BinaryCell) {
          digest = handleBinaryCell(cell, cellDigest);
        } else if (cell instanceof SimpleCell) {
          if (cell.getMessageDigest() != null) {
            digest = cell.getMessageDigest();
          } else {
            digest = cellDigest.digest(((SimpleCell) cell).getSimpleData().getBytes(StandardCharsets.UTF_8));
          }
        } else if (cell instanceof NullCell) {
          digest = cellDigest.digest();
        } else {
          throw new ModuleException()
            .withMessage(cell.getClass().getSimpleName() + " is not supported type for Merkle tree calculation");
        }

        final String cellDigestHex = MessageDigestUtils.getHexFromMessageDigest(digest, lowerCase);
        rowDigest.update(cellDigestHex.getBytes());
        if (explain) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeObjectField(INDEX_FIELD_NAME, cell.getId());
          jsonGenerator.writeObjectField(CELL_HASH_FIELD_NAME, cellDigestHex);
          jsonGenerator.writeEndObject();
        }
      }

      final String rowDigestHex = MessageDigestUtils.getHexFromMessageDigest(rowDigest.digest(), lowerCase);
      tableDigest.update(rowDigestHex.getBytes());
      if (explain) {
        jsonGenerator.writeEndArray();
        jsonGenerator.writeObjectField(ROW_HASH_FIELD_NAME, rowDigestHex);
        jsonGenerator.writeEndObject();
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }

    this.exportModule.handleDataRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    final String tableDigestHex = MessageDigestUtils.getHexFromMessageDigest(tableDigest.digest(), lowerCase);
    schemaDigest.update(tableDigestHex.getBytes());
    merkleColumnsIndexes = new ArrayList<>();

    try {
      if (explain) {
        jsonGenerator.writeEndArray();
        jsonGenerator.writeObjectField(TABLE_HASH_FIELD_NAME, tableDigestHex);
      }

      jsonGenerator.writeEndObject();
      jsonGenerator.writeEndObject();

    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }

    this.exportModule.handleDataCloseTable(tableId);
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    final String schemaDigestHex = MessageDigestUtils.getHexFromMessageDigest(schemaDigest.digest(), lowerCase);
    databaseDigest.update(schemaDigestHex.getBytes());

    try {
      jsonGenerator.writeEndArray();
      if (explain) {
        jsonGenerator.writeObjectField(SCHEMA_HASH_FIELD_NAME, schemaDigestHex);
      }
      jsonGenerator.writeEndObject();
    } catch (IOException e) {
      throw new ModuleException().withMessage(UNABLE_TO_WRITE_TO_THE_OUTPUT_FILE).withCause(e);
    }

    this.exportModule.handleDataCloseSchema(schemaName);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    final String databaseDigestHex = MessageDigestUtils.getHexFromMessageDigest(databaseDigest.digest(), lowerCase);
    try {
      jsonGenerator.writeEndObject();
      jsonGenerator.writeEndArray();

      jsonGenerator.writeObjectField(TOP_HASH_FIELD_NAME, databaseDigestHex);

      jsonGenerator.writeEndObject();
      jsonGenerator.writeEndObject();
      jsonGenerator.close();
      outputStream.close();
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Could not close the JSON file '" + outputFile.normalize().toAbsolutePath().toString() + "'")
        .withCause(e);
    }

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

  private MessageDigest getMessageDigest() throws ModuleException {
    try {
      return MessageDigest.getInstance(messageDigestAlgorithm);
    } catch (NoSuchAlgorithmException ex) {
      throw new ModuleException().withMessage(ex.getMessage()).withCause(ex);
    }
  }

  private byte[] handleBinaryCell(Cell cell, MessageDigest cellDigest) throws ModuleException {
    BinaryCell binaryCell = (BinaryCell) cell;

    if (binaryCell.getMessageDigest() != null && binaryCell.getDigestAlgorithm().equalsIgnoreCase(messageDigestAlgorithm)) {
      return binaryCell.getMessageDigest();
    } else {
      if (binaryCell.getSize() <= 0) {
        return cellDigest.digest();
      } else {
        InputStream inputStream = binaryCell.createInputStream();
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        return MessageDigestUtils.digestStream(cellDigest, bufferedInputStream);
      }
    }
  }
}