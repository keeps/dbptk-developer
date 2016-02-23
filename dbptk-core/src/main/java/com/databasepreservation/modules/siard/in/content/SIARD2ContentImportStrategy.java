package com.databasepreservation.modules.siard.in.content;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Stack;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentImportStrategy extends DefaultHandler implements ContentImportStrategy {
  // SAXHandler settings
  static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
  static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
  static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";
  // Keywords
  private static final String SCHEMA_KEYWORD = "schema";
  private static final String TABLE_KEYWORD = "table";
  private static final String COLUMN_KEYWORD = "c";
  private static final String ROW_KEYWORD = "row";
  private static final String FILE_KEYWORD = "file";
  private final CustomLogger logger = CustomLogger.getLogger(SIARD1ContentImportStrategy.class);
  // ImportStrategy
  private final ContentPathImportStrategy contentPathStrategy;
  private final ReadStrategy readStrategy;
  private final Stack<String> tagsStack = new Stack<String>();
  private final StringBuilder tempVal = new StringBuilder();
  private SIARDArchiveContainer contentContainer;
  private DatabaseExportModule databaseExportModule;
  private SAXErrorHandler errorHandler;
  // SAXHandler state
  private TableStructure currentTable;
  private SchemaStructure currentSchema;
  private InputStream currentTableStream;
  private BinaryCell currentBinaryCell;
  private Row row;
  private int rowIndex;

  public SIARD2ContentImportStrategy(ReadStrategy readStrategy, ContentPathImportStrategy contentPathStrategy) {
    this.contentPathStrategy = contentPathStrategy;
    this.readStrategy = readStrategy;
  }

  @Override
  public void importContent(DatabaseExportModule handler, SIARDArchiveContainer container,
    DatabaseStructure databaseStructure) throws ModuleException {
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
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      currentSchema = schema;
      for (TableStructure table : schema.getTables()) {

        // setup a new validating parser
        InputStream xsdStream = readStrategy.createInputStream(container,
          contentPathStrategy.getTableXSDFilePath(schema.getName(), table.getId()));

        try {
          saxParser = saxParserFactory.newSAXParser();
          saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
          saxParser.setProperty(JAXP_SCHEMA_SOURCE, xsdStream);
        } catch (SAXException e) {
          logger.error("Error validating schema", e);
        } catch (ParserConfigurationException e) {
          logger.error("Error creating XML SAXparser", e);
        }

        // import values from XML
        currentTableStream = readStrategy.createInputStream(container,
          contentPathStrategy.getTableXMLFilePath(schema.getName(), table.getId()));

        currentTable = table;

        errorHandler = new SAXErrorHandler();

        try {
          XMLReader xmlReader = saxParser.getXMLReader();
          xmlReader.setContentHandler(this);
          xmlReader.setErrorHandler(errorHandler);
          xmlReader.parse(new InputSource(currentTableStream));
        } catch (SAXException e) {
          throw new ModuleException("A SAX error occurred during processing of XML table file", e);
        } catch (IOException e) {
          throw new ModuleException("Error while reading XML table file", e);
        }

        if (errorHandler.hasError()) {
          throw new ModuleException(
            "Parsing or validation error occurred while reading XML table file (details are above)");
        }

        try {
          currentTableStream.close();
        } catch (IOException e) {
          throw new ModuleException("Could not close XML table input stream", e);
        }

        try {
          xsdStream.close();
        } catch (IOException e) {
          throw new ModuleException("Could not close table XSD schema input stream", e);
        }
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

    if (qName.equalsIgnoreCase(SCHEMA_KEYWORD)) {
      try {
        databaseExportModule.handleDataOpenSchema(currentSchema.getName());
      } catch (ModuleException e) {
        logger.error("An error occurred while handling data open schema", e);
      }
    } else if (qName.equalsIgnoreCase(TABLE_KEYWORD)) {
      this.rowIndex = 0;
      try {
        databaseExportModule.handleDataOpenTable(currentTable.getId());
      } catch (ModuleException e) {
        logger.error("An error occurred while handling data open table", e);
      }
    } else if (qName.equalsIgnoreCase(ROW_KEYWORD)) {
      row = new Row();
      row.setCells(new ArrayList<Cell>());
      for (int i = 0; i < currentTable.getColumns().size(); i++) {
        row.getCells().add(null);
      }
    } else if (qName.startsWith(COLUMN_KEYWORD)) {
      if (attr.getValue(FILE_KEYWORD) != null) {
        String lobDirLastPart = attr.getValue(FILE_KEYWORD);
        int columnIndex = Integer.parseInt(qName.substring(1));

        String lobPath = contentPathStrategy.getLobPath(null, currentSchema.getName(), currentTable.getId(),
          currentTable.getColumns().get(columnIndex - 1).getId(), lobDirLastPart);

        try {
          FileItem fileItem = new FileItem(readStrategy.createInputStream(contentContainer, lobPath));
          currentBinaryCell = new BinaryCell(
          // TODO: what about CLOBs? should they also created as BinaryCells?
            String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex), fileItem);
        } catch (ModuleException e) {
          logger.error("Failed to open lob at " + lobPath, e);
        }

        logger.debug(String.format("Binary cell %s on row #%d with lob dir %s", currentBinaryCell.getId(), rowIndex,
          lobPath));
      } else {
        currentBinaryCell = null;
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

    if (tag.equalsIgnoreCase(SCHEMA_KEYWORD)) {
      try {
        databaseExportModule.handleDataCloseSchema(currentSchema.getName());
      } catch (ModuleException e) {
        logger.error("An error occurred while handling data close schema", e);
      }
    } else if (tag.equalsIgnoreCase(TABLE_KEYWORD)) {
      try {
        logger.debug("before handle data close");
        databaseExportModule.handleDataCloseTable(currentTable.getId());
      } catch (ModuleException e) {
        logger.error("An error occurred while handling data close table", e);
      }
    } else if (tag.equalsIgnoreCase(ROW_KEYWORD)) {
      // assume all cells that are not present are null
      for (int i = row.getCells().size() - 1; i >= 0; i--) {
        Cell cell = row.getCells().get(i);
        if (cell == null) {
          String id = String.format("%s.%d", currentTable.getColumns().get(i).getId(), rowIndex);
          SimpleCell simpleCell = new SimpleCell(id);
          simpleCell.setSimpledata(null);
          row.getCells().set(i, simpleCell);
        }
      }

      row.setIndex(rowIndex);
      rowIndex++;
      try {
        databaseExportModule.handleDataRow(row);
      } catch (InvalidDataException e) {
        logger.error("An error occurred while handling data row", e);
      } catch (ModuleException e) {
        logger.error("An error occurred while handling data row", e);
      }
    } else if (tag.contains(COLUMN_KEYWORD)) {
      // TODO Support other cell types
      String[] subStrings = tag.split(COLUMN_KEYWORD);
      Integer columnIndex = Integer.valueOf(subStrings[1]);
      Type type = currentTable.getColumns().get(columnIndex - 1).getType();

      if (type instanceof SimpleTypeString) {
        localVal = SIARDHelper.decode(localVal);
      }

      Cell cell = null;
      if (currentBinaryCell != null) {
        cell = currentBinaryCell;
      } else {
        String id = String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex);

        if (type instanceof SimpleTypeBinary && StringUtils.isNotBlank(localVal)) {
          // binary data with less than 2000 bytes does not have its own file
          try {
            InputStream is = new ByteArrayInputStream(Hex.decodeHex(localVal.toCharArray()));
            cell = new BinaryCell(id, new FileItem(is));
          } catch (ModuleException e) {
            logger.error("An error occurred while importing in-table binary cell", e);
          } catch (DecoderException e) {
            logger.error(String.format("Illegal characters in hexadecimal string \"%s\"", localVal), e);
          }
        } else {
          cell = new SimpleCell(id);
          ((SimpleCell) cell).setSimpledata(localVal);
        }
      }
      row.getCells().set(columnIndex - 1, cell);
    }
  }

  @Override
  public void characters(char buf[], int offset, int len) {
    tempVal.append(buf, offset, len);
  }
}
