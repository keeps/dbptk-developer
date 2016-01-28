package com.databasepreservation.modules.siard.in.content;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer.OutputContainerType;
import com.databasepreservation.modules.siard.in.path.SIARDDKContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

public class SIARDDKContentImportStrategy extends DefaultHandler implements ContentImportStrategy {

  private final CustomLogger logger = CustomLogger.getLogger(SIARDDKContentImportStrategy.class);
  protected final ReadStrategy readStrategy;
  protected final SIARDDKContentPathImportStrategy contentPathStrategy;
  protected final String importAsSchema;
  protected static final String XML_TBL_TAG_LOCALNAME = "table";
  protected static final String XML_ROW_TAG_LOCALNAME = "row";
  protected static final Pattern XML_ROW_COLUMN_LOCALNAME_PATTERN = Pattern.compile("c([1-9][0-9]*)");

  static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
  static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource";
  protected DatabaseExportModule dbExportHandler;
  protected TableStructure currentTable;
  protected String currentTagLocalName;
  private StringBuilder currentTagContentStrBld;
  protected boolean isInTblTag;
  protected boolean isInRowTag;
  protected boolean isInCellTag;
  protected boolean isInNullValueCell;
  protected Row currentRow;
  protected Cell[] currentRowCells;
  protected int rowIndex = 0;
  private static final String SIARDDK_NIL_LOCAL_ATTR_NAME = "nil";

  /**
   * @author Thomas Kristensen <tk@bithuset.dk>
   *
   */
  public SIARDDKContentImportStrategy(ReadStrategy readStrategy, SIARDDKContentPathImportStrategy contentPathStrategy,
    String importAsSchema) {
    this.readStrategy = readStrategy;
    this.contentPathStrategy = contentPathStrategy;
    this.importAsSchema = importAsSchema;

  }

  @Override
  public void importContent(DatabaseExportModule dbExportHandler, SIARDArchiveContainer mainFolder,
    DatabaseStructure databaseStructure) throws ModuleException {
    contentPathStrategy.parseFileIndexMetadata();
    this.dbExportHandler = dbExportHandler;
    Map<Path, SIARDArchiveContainer> archiveContainerByPath = new HashMap<Path, SIARDArchiveContainer>();
    archiveContainerByPath.put(mainFolder.getPath(), mainFolder);
    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    saxParserFactory.setValidating(true);
    saxParserFactory.setNamespaceAware(true);
    SAXParser saxParser = null;
    InputStream xsdStream = null;
    SIARDArchiveContainer currentFolder = null;
    assert (databaseStructure.getSchemas().size() == 1);
    this.dbExportHandler.handleDataOpenSchema(importAsSchema);
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      assert (schema.getName().equals(importAsSchema));
      for (TableStructure table : schema.getTables()) {
        currentTable = table;
        this.dbExportHandler.handleDataOpenTable(table.getId());
        rowIndex = 0;
        String xsdFileName = contentPathStrategy.getTableXSDFilePath(schema.getName(), table.getId());
        String xmlFileName = contentPathStrategy.getTableXMLFilePath(schema.getName(), table.getId());
          Path archiveFolderLogicalPath = contentPathStrategy.getArchiveFolderPath(importAsSchema, table.getId());
          Path archiveFolderActualPath = mainFolder.getPath().resolveSibling(archiveFolderLogicalPath);
          if (!archiveContainerByPath.containsKey(archiveFolderActualPath)) {
            archiveContainerByPath.put(mainFolder.getPath().resolveSibling(archiveFolderLogicalPath),
              // TODO: Verify meaning of OutputContainerType. AUX never used.
              new SIARDArchiveContainer(archiveFolderActualPath, OutputContainerType.MAIN));
          }
          currentFolder = archiveContainerByPath.get(archiveFolderActualPath);
        try {
          xsdStream = readStrategy.createInputStream(currentFolder,
 xsdFileName);
          saxParser = saxParserFactory.newSAXParser();
          // saxParser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, ""); //TODO
          // saxParser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
          // //TODO
          // TODO: Verify that this does not undo the wanted behavior defined
          // below
          saxParser.setProperty(JAXP_SCHEMA_LANGUAGE, XMLConstants.W3C_XML_SCHEMA_NS_URI);
          saxParser.setProperty(JAXP_SCHEMA_SOURCE, xsdStream);
        } catch (SAXException e) {
          logger.error("Error validating schema", e);
          throw new ModuleException("Error reading XSD file: "
            + contentPathStrategy.getTableXSDFilePath(schema.getName(), table.getId()) + " for table:" + table.getId(),
            e);
        } catch (ParserConfigurationException e) {
          logger.error("Error creating XML SAXparser", e);
          throw new ModuleException(e);
        }

        InputStream currentTableStream = readStrategy.createInputStream(currentFolder,
 xmlFileName);

        SAXErrorHandler saxErrorHandler = new SAXErrorHandler();

        try {
          XMLReader xmlReader = saxParser.getXMLReader();
          xmlReader.setContentHandler(this);
          xmlReader.setErrorHandler(saxErrorHandler);
          logger.debug("begin parse of xml-file:[" + xmlFileName + "], using xsd [" + xsdFileName + "]");
          xmlReader.parse(new InputSource(currentTableStream));

        } catch (SAXException e) {
          throw new ModuleException(
            "A SAX error occurred during processing of XML table file for table:" + table.getId(), e);
        } catch (IOException e) {
          throw new ModuleException("Error while reading XML table file for table:" + table.getId(), e);
        }

        if (saxErrorHandler.hasError()) {
          throw new ModuleException(
            "Parsing or validation error occurred while reading XML table file for table:" + table.getId());
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
        this.dbExportHandler.handleDataCloseTable(table.getId());
      }
      this.dbExportHandler.handleDataCloseSchema(importAsSchema);
    }

  }

  @Override
  public void startDocument() throws SAXException {
    isInTblTag = false;
    isInRowTag = false;
    isInCellTag = false;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    currentTagContentStrBld = new StringBuilder();
    currentTagLocalName = localName.toLowerCase();

    if (currentTagLocalName.equals(XML_TBL_TAG_LOCALNAME)) {
      isInTblTag = true;

    } else {
      if (isInTblTag && currentTagLocalName.equals(XML_ROW_TAG_LOCALNAME)) {
        isInRowTag = true;
        currentRow = new Row();
        currentRow.setIndex(rowIndex);
        rowIndex++;
        currentRowCells = new Cell[currentTable.getColumns().size()];

      } else {
        if (isInTblTag && isInRowTag) {
          Matcher matcher = XML_ROW_COLUMN_LOCALNAME_PATTERN.matcher(localName);
          if (matcher.matches()) {
            isInCellTag = true;
            int nilAttrIndex = attributes.getIndex(XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI,
              SIARDDK_NIL_LOCAL_ATTR_NAME);
            isInNullValueCell = nilAttrIndex != -1 && Boolean.valueOf(attributes.getValue(nilAttrIndex));
          }
        }

      }
    }
  }


  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {

    if (localName.equals(XML_TBL_TAG_LOCALNAME)) {
      isInTblTag = false;

    } else {
      if (isInTblTag && localName.equals(XML_ROW_TAG_LOCALNAME)) {
        currentRow.setCells(Arrays.asList(currentRowCells));
        try {
          this.dbExportHandler.handleDataRow(currentRow);
        } catch (InvalidDataException e) {
          // TODO: Add row index to description
          throw new SAXException(e);
        } catch (ModuleException e) {
          // TODO: Add row index to description
          throw new SAXException(e);
        }

        isInRowTag = false;
      } else {
        Matcher matcher = XML_ROW_COLUMN_LOCALNAME_PATTERN.matcher(localName);
        if (isInCellTag && matcher.matches()) {
          Integer columnIndex = Integer.valueOf(matcher.group(1));

          // TODO: Handle LOB cells & simple binary cells

          String id = String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex);
          SimpleCell cell = new SimpleCell(id);
          if (isInNullValueCell) {
            cell.setSimpledata(null);
          } else {
            String preparedCellVal = currentTagContentStrBld.toString().trim();
            Type currentCellType = currentTable.getColumns().get(columnIndex - 1).getType();
          if (currentCellType instanceof SimpleTypeString) {
              // TODO: Establish SIARD-DK requirements here.
              preparedCellVal = SIARDHelper.decode(preparedCellVal);
          }
            cell.setSimpledata(preparedCellVal);
          }
          currentRowCells[columnIndex - 1] = cell;
          // TODO: Verify all cells were present.
          isInCellTag = false;
          isInNullValueCell = false;
        }

      }
    }

  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    currentTagContentStrBld.append(ch, start, length);
  }

}
