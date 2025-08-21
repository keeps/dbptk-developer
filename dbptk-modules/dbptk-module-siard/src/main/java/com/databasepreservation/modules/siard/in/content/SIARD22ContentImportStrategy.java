/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.content;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.common.io.providers.DummyInputStreamProvider;
import com.databasepreservation.common.io.providers.SegmentedPathInputStreamProvider;
import com.databasepreservation.model.data.ArrayCell;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD22ContentPathExportStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD22ContentImportStrategy extends DefaultHandler implements ContentImportStrategy {
  // SAXHandler settings
  static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
  static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
  static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";
  // Keywords
  private static final String SCHEMA_KEYWORD = "schema";
  private static final String TABLE_KEYWORD = "table";
  private static final String COLUMN_KEYWORD = "c";
  private static final String ARRAY_KEYWORD = "a";
  private static final String ROW_KEYWORD = "row";
  private static final String FILE_KEYWORD = "file";
  private static final String LENGTH_KEYWORD = "length";
  private static final String DIGEST_KEYWORD = "digest";
  private static final String DIGEST_TYPE_KEYWORD = "digestType";
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD22ContentImportStrategy.class);
  // ImportStrategy
  private final ContentPathImportStrategy contentPathStrategy;
  private final ReadStrategy readStrategy;
  private final boolean ignoreLobs;
  private final Deque<String> tagsStack = new LinkedList<>();
  private final StringBuilder tempVal = new StringBuilder();
  private SIARDArchiveContainer contentContainer;
  private SIARDArchiveContainer lobContainer;
  private DatabaseExportModule databaseExportModule;
  private SAXErrorHandler errorHandler;
  // SAXHandler state
  private TableStructure currentTable;
  private SchemaStructure currentSchema;
  private DatabaseStructure database;
  private InputStream currentTableStream;
  private BinaryCell currentBlobCell;
  private SimpleCell currentClobCell;
  private ArrayCell currentArrayCell;
  private int currentColumnIndex;
  private Row row;
  private long rowIndex;
  private boolean useLobPathFallback = false;

  public SIARD22ContentImportStrategy(ReadStrategy readStrategy, ContentPathImportStrategy contentPathStrategy,
    SIARDArchiveContainer lobContainer, boolean ignoreLobs) {
    this.contentPathStrategy = contentPathStrategy;
    this.readStrategy = readStrategy;
    this.lobContainer = lobContainer;
    this.ignoreLobs = ignoreLobs;
  }

  @Override
  public void importContent(DatabaseExportModule handler, SIARDArchiveContainer container,
    DatabaseStructure databaseStructure, ModuleConfiguration moduleSettings) throws ModuleException {
    // set instance state
    this.databaseExportModule = handler;
    this.contentContainer = container;

    // pre-setup parser and validation
    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    SAXParserFactory saxParserFactory = null;
    saxParserFactory = SAXParserFactory.newInstance();
    saxParserFactory.setValidating(true);
    saxParserFactory.setNamespaceAware(true);
    SAXParser saxParser = null;

    // process tables
    this.database = databaseStructure;
    long completedSchemas = 0;
    long completedTablesInSchema;
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      boolean schemaHandled = false;
      currentSchema = schema;
      completedTablesInSchema = 0;
      try {
        databaseExportModule.handleDataOpenSchema(currentSchema.getName());
        schemaHandled = true;
      } catch (ModuleException e) {
        LOGGER.error("An error occurred while handling data open schema", e);
      }

      if (schemaHandled) {
        for (TableStructure table : schema.getTables()) {
          currentTable = table;
          boolean tableHandled = false;
          this.rowIndex = 1;
          try {
            databaseExportModule.handleDataOpenTable(currentTable.getId());
            tableHandled = true;
          } catch (ModuleException e) {
            LOGGER.error("An error occurred while handling data open table", e);
          }
          long currentTableTotalRows = currentTable.getRows();

          if (tableHandled && moduleSettings.isFetchRows()) {
            InputStream xsdStream = null;
            try {
              // setup a new validating parser
              xsdStream = readStrategy.createInputStream(container,
                contentPathStrategy.getTableXSDFilePath(schema.getName(), currentTable.getId()));

              try {
                saxParser = saxParserFactory.newSAXParser();
                saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
                saxParser.setProperty(JAXP_SCHEMA_SOURCE, xsdStream);
              } catch (SAXException e) {
                LOGGER.error("Error validating schema", e);
              } catch (ParserConfigurationException e) {
                LOGGER.error("Error creating XML SAXparser", e);
              }

              // import values from XML
              String tableFilename = contentPathStrategy.getTableXMLFilePath(schema.getName(), currentTable.getId());
              currentTableStream = readStrategy.createInputStream(container, tableFilename);

              errorHandler = new SAXErrorHandler();

              try {
                XMLReader xmlReader = saxParser.getXMLReader();
                xmlReader.setContentHandler(this);
                xmlReader.setErrorHandler(errorHandler);
                InputStreamReader tableInputStreamReader = new InputStreamReader(new BOMInputStream(currentTableStream),
                  StandardCharsets.UTF_8);
                InputSource tableInputSource = new InputSource(tableInputStreamReader);
                tableInputSource.setEncoding("UTF-8");
                xmlReader.parse(tableInputSource);
              } catch (SAXException e) {
                throw new ModuleException()
                  .withMessage("A SAX error occurred during processing of XML table file at " + tableFilename)
                  .withCause(e);
              } catch (IOException e) {
                throw new ModuleException().withMessage("Error while reading XML table file").withCause(e);
              }

              if (errorHandler.hasError()) {
                throw new ModuleException()
                  .withMessage("Parsing or validation error occurred while reading XML table file (details are above)");
              }

              try {
                currentTableStream.close();
              } catch (IOException e) {
                throw new ModuleException().withMessage("Could not close XML table input stream").withCause(e);
              }

              try {
                xsdStream.close();
              } catch (IOException e) {
                throw new ModuleException().withMessage("Could not close table XSD schema input stream").withCause(e);
              }
            } catch (ModuleException e) {
              LOGGER.error("An error occurred converting table contents", e);
            } finally {
              try {
                if (xsdStream != null) {
                  xsdStream.close();
                }
              } catch (IOException e) {
                LOGGER.debug("Could not close the stream after an error occurred", e);
              }
            }
          }

          LOGGER.debug("Total of {} row(s) processed", rowIndex);

          completedTablesInSchema++;
          try {
            databaseExportModule.handleDataCloseTable(currentTable.getId());
          } catch (ModuleException e) {
            LOGGER.error("An error occurred while handling data close table", e);
          }
        }
      }

      completedSchemas++;
      try {
        databaseExportModule.handleDataCloseSchema(currentSchema.getName());
      } catch (ModuleException e) {
        LOGGER.error("An error occurred while handling data close schema", e);
      }
    }
  }

  private void pushTag(String tag) {
    tagsStack.push(tag);
  }

  private String popTag() {
    return tagsStack.pop();
  }

  private String peekTag() {
    return tagsStack.peek();
  }

  @Override
  public void startDocument() throws SAXException {
    pushTag("");
  }

  @Override
  public void endDocument() throws SAXException {
    // nothing to do
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attr) {
    pushTag(qName);
    tempVal.setLength(0);

    if (qName.equalsIgnoreCase(ROW_KEYWORD)) {
      row = new Row();
      row.setCells(new ArrayList<Cell>());
      for (int i = 0; i < currentTable.getColumns().size(); i++) {
        row.getCells().add(null);
      }
    } else if (qName.startsWith(COLUMN_KEYWORD)) {
      currentColumnIndex = Integer.parseInt(qName.substring(1));
      ColumnStructure currentColumn = database.getSchemaByName(currentSchema.getName())
        .getTableById(currentTable.getId()).getColumns().get(currentColumnIndex - 1);

      if (attr.getValue(FILE_KEYWORD) != null) {
        String lobsDir = database.getLobFolder();
        if (lobsDir == null) {
          lobsDir = "";
        }
        String columnLobsDir = currentColumn.getLobFolder();
        if (columnLobsDir == null) {
          columnLobsDir = "";
        }
        String lobFile = attr.getValue(FILE_KEYWORD);
        String lobPath;
        if (lobFile.startsWith(File.separator)) {
          lobPath = lobFile;
        } else if (columnLobsDir.startsWith(File.separator)) {
          lobPath = Path.of(columnLobsDir, lobFile).toString();
        } else {
          lobPath = Path.of(lobsDir, columnLobsDir, lobFile).toString();
        }

        SIARDArchiveContainer container;
        if (lobPath.startsWith("../")) {
          container = lobContainer;
          lobPath = lobPath.substring(3);
        } else {
          container = contentContainer;
        }

        InputStream inputStream = null;

        try {
          if (lobPath.endsWith(SIARD22ContentPathExportStrategy.BLOB_EXTENSION)) {
            // assuming auxiliary containers are in a directory, use the
            // existing LOB file instead of copying it to a temporary directory
            if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.AUXILIARY)) {
              LOGGER.debug("lobContainer: {}\ncontentContainer: {}", lobContainer, contentContainer);
              currentBlobCell = new BinaryCell(
                currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex,
                new SegmentedPathInputStreamProvider(container.getPath().resolve(Paths.get(lobPath))));
            } else {
              if (ignoreLobs) {
                Optional<Long> optionalLength = extractLengthFromBinaryColumn(attr);
                Optional<String> optionalDigest = extractDigestFromBinaryColumn(attr);
                Optional<String> optionalDigestType = extractDigestTypeFromBinaryColumn(attr);

                currentBlobCell = new BinaryCell(
                  currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex,
                  new DummyInputStreamProvider(), lobPath, optionalLength.orElse(0L), optionalDigest.orElse(null),
                  optionalDigestType.orElse(null));
              } else {
                if (lobPath.startsWith(File.separator)) {
                  inputStream = Files.newInputStream(Paths.get(lobPath));
                } else {
                  inputStream = createInputStream(container, lobPath);
                }
                currentBlobCell = new BinaryCell(
                  currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex, inputStream);
              }
            }

            LOGGER.debug(
              String.format("BLOB cell %s on row #%d with lob dir %s", currentBlobCell.getId(), rowIndex, lobPath));
          } else if (lobPath.endsWith(SIARD22ContentPathExportStrategy.CLOB_EXTENSION)) {
            Path inputStreamPath;
            if (lobPath.startsWith(File.separator)) {
              inputStreamPath = Paths.get(lobPath);
            } else {
              inputStreamPath = container.getPath().resolve(Paths.get(lobPath));
            }
            SegmentedPathInputStreamProvider inputStreamProvider = new SegmentedPathInputStreamProvider(
              inputStreamPath);
            inputStream = inputStreamProvider.createInputStream();
            String data = IOUtils.toString(inputStream);
            currentClobCell = new SimpleCell(
              currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex, data);

            LOGGER.debug("CLOB cell {} on row #{} with lob path {}", currentClobCell.getId(), rowIndex, lobPath);
          }
        } catch (ModuleException | IOException e) {
          LOGGER.error("Failed to open lob at {}", lobPath, e);
        } finally {
          try {
            if (inputStream != null)
              inputStream.close();
          } catch (IOException e) {
            LOGGER.debug("Could not close the stream after an error occurred", e);
          }
        }
      }
    } else if (qName.startsWith(ARRAY_KEYWORD)) {
      if (currentArrayCell == null && qName.equalsIgnoreCase(ARRAY_KEYWORD + "1")) {
        currentArrayCell = new ArrayCell(
          currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex);
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    String tag = peekTag();
    if (!qName.equals(tag)) {
      throw new InternalError();
    }

    String localVal = tempVal.toString();

    popTag();

    if (tag.equalsIgnoreCase(ROW_KEYWORD)) {
      // assume all cells that are not present are null
      for (int i = row.getCells().size() - 1; i >= 0; i--) {
        Cell cell = row.getCells().get(i);
        if (cell == null) {
          String id = currentTable.getColumns().get(i).getId() + "." + rowIndex;
          row.getCells().set(i, new NullCell(id));
        }
      }

      row.setIndex(rowIndex);
      rowIndex++;
      try {
        databaseExportModule.handleDataRow(row);
      } catch (ModuleException e) {
        LOGGER.error("An error occurred while handling data row", e);
      }
    } else if (tag.startsWith(COLUMN_KEYWORD) && currentArrayCell != null) {
      if (currentArrayCell.isEmpty()) {
        row.getCells().set(currentColumnIndex - 1, new NullCell(currentArrayCell.getId()));
      } else {
        row.getCells().set(currentColumnIndex - 1, currentArrayCell);
      }
      currentArrayCell = null;
    } else if (tag.startsWith(ARRAY_KEYWORD) && currentArrayCell != null) {
      Type type = currentTable.getColumns().get(currentColumnIndex - 1).getType();
      Integer arrayPosition = Integer.parseInt(qName.substring(ARRAY_KEYWORD.length()));
      Cell cell = null;

      List<Integer> arrayCellPositions = getArrayCellPositions(arrayPosition, tagsStack);

      // avoid trying to save a new array element when closing parent xml tags in a
      // multidimensional array situation
      if (currentArrayCell.isEmpty() || currentArrayCell.calculateDimensions() == arrayCellPositions.size()) {

        if (type instanceof ComposedTypeArray) {
          ComposedTypeArray arrayType = (ComposedTypeArray) type;

          String subId = currentArrayCell.getId() + "." + StringUtils.join(arrayCellPositions, '.');

          if (arrayType.getElementType() instanceof SimpleTypeString) {
            localVal = XMLUtils.decode(localVal);
            cell = new SimpleCell(subId, localVal);
          } else if (arrayType.getElementType() instanceof SimpleTypeBinary) {
            if (StringUtils.isNotBlank(localVal)) {
              // binary data with less than 2000 bytes does not have its own file
              try {
                InputStream is = new ByteArrayInputStream(Hex.decodeHex(localVal.toCharArray()));
                cell = new BinaryCell(subId, is);
              } catch (ModuleException e) {
                LOGGER.error("An error occurred while importing in-table binary cell", e);
              } catch (DecoderException e) {
                LOGGER.error(String.format("Illegal characters in hexadecimal string \"%s\"", localVal), e);
              }
            } else {
              cell = new SimpleCell(subId, localVal);
            }
          } else if (StringUtils.isNotBlank(localVal)) {
            cell = new SimpleCell(subId, localVal);
          }
        }

        currentArrayCell.put(cell, arrayCellPositions);
      }
    } else if (tag.startsWith(COLUMN_KEYWORD)) {
      // TODO Support other cell types
      Type type = currentTable.getColumns().get(currentColumnIndex - 1).getType();

      if (type instanceof SimpleTypeString) {
        localVal = XMLUtils.decode(localVal);
      }

      Cell cell = null;
      if (currentBlobCell != null) {
        cell = currentBlobCell;
        currentBlobCell = null;
      } else if (currentClobCell != null) {
        cell = currentClobCell;
        currentClobCell = null;
      } else {
        String id = currentTable.getColumns().get(currentColumnIndex - 1).getId() + "." + rowIndex;

        if (type instanceof SimpleTypeBinary && StringUtils.isNotBlank(localVal)) {
          // binary data with less than 2000 bytes does not have its own file
          try {
            InputStream is = new ByteArrayInputStream(Hex.decodeHex(localVal.toCharArray()));
            cell = new BinaryCell(id, is);
          } catch (ModuleException e) {
            LOGGER.error("An error occurred while importing in-table binary cell", e);
          } catch (DecoderException e) {
            LOGGER.error(String.format("Illegal characters in hexadecimal string \"%s\"", localVal), e);
          }
        } else {
          cell = new SimpleCell(id, localVal);
        }
      }
      row.getCells().set(currentColumnIndex - 1, cell);
    }
    tempVal.setLength(0);
  }

  @Override
  public void characters(char buf[], int offset, int len) {
    tempVal.append(buf, offset, len);
  }

  private InputStream createInputStream(SIARDArchiveContainer container, String lobPath, String lobDir)
    throws ModuleException {
    String lobName = Paths.get(lobDir).getFileName().toString();
    if (useLobPathFallback) {
      lobPath = contentPathStrategy.getLobPathFallback(null,
        currentTable.getColumns().get(currentColumnIndex - 1).getId(), lobName);
    }
    try {
      return readStrategy.createInputStream(container, lobPath);
    } catch (ModuleException e) {
      useLobPathFallback = true;
      lobPath = contentPathStrategy.getLobPathFallback(null,
        currentTable.getColumns().get(currentColumnIndex - 1).getId(), lobName);
      return readStrategy.createInputStream(container, lobPath);
    }
  }

  private InputStream createInputStream(SIARDArchiveContainer container, String lobPath) throws ModuleException {
    String lobName = Paths.get(lobPath).getFileName().toString();
    if (useLobPathFallback) {
      lobPath = contentPathStrategy.getLobPathFallback(null,
        currentTable.getColumns().get(currentColumnIndex - 1).getId(), lobName);
    }
    try {
      return readStrategy.createInputStream(container, lobPath);
    } catch (ModuleException e) {
      useLobPathFallback = true;
      lobPath = contentPathStrategy.getLobPathFallback(null,
        currentTable.getColumns().get(currentColumnIndex - 1).getId(), lobName);
      return readStrategy.createInputStream(container, lobPath);
    }
  }

  private List<Integer> getArrayCellPositions(Integer current, Deque<String> parentTags) {
    List<Integer> positions = new ArrayList<>();

    Iterator<String> iterator = parentTags.iterator();
    while (iterator.hasNext()) {
      String tag = iterator.next();
      if (StringUtils.isNotBlank(tag) && tag.startsWith(ARRAY_KEYWORD)) {
        Integer position = Integer.parseInt(tag.substring(1));
        positions.add(position);
      } else {
        break;
      }
    }

    Collections.reverse(positions);
    positions.add(current);

    return positions;
  }

  private Optional<Long> extractLengthFromBinaryColumn(Attributes attr) {
    String value = attr.getValue(LENGTH_KEYWORD);
    try {
      if (value != null) {
        return Optional.of(Long.parseLong(value));
      }
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
    return Optional.empty();
  }

  private Optional<String> extractDigestFromBinaryColumn(Attributes attr) {
    String value = attr.getValue(DIGEST_KEYWORD);

    if (value != null) {
      return Optional.of(value);
    }

    return Optional.empty();
  }

  private Optional<String> extractDigestTypeFromBinaryColumn(Attributes attr) {
    String value = attr.getValue(DIGEST_TYPE_KEYWORD);

    if (value != null) {
      return Optional.of(value);
    }

    return Optional.empty();
  }
}
