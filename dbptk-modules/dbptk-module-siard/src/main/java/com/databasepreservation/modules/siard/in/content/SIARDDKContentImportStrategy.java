package com.databasepreservation.modules.siard.in.content;

import java.io.IOException;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.TypeInfoProvider;
import javax.xml.validation.ValidatorHandler;

import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.TypeInfo;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.common.ObservableModule;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer.OutputContainerType;
import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.utils.XMLUtils;

public class SIARDDKContentImportStrategy extends DefaultHandler implements ContentImportStrategy {
  private static final Logger logger = LoggerFactory.getLogger(SIARDDKContentImportStrategy.class);
  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDDKPathImportStrategy pathStrategy;
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
  protected int rowIndex = 1;
  private static final String SIARDDK_NIL_LOCAL_ATTR_NAME = "nil";
  protected TypeInfoProvider typeInfoProvider;
  protected TypeInfo xsdCellType;
  private ObservableModule observable;
  private DatabaseStructure databaseStructure;

  /**
   * @author Thomas Kristensen <tk@bithuset.dk>
   *
   */
  public SIARDDKContentImportStrategy(FolderReadStrategyMD5Sum readStrategy, SIARDDKPathImportStrategy pathStrategy,
    String importAsSchema) {
    this.readStrategy = readStrategy;
    this.pathStrategy = pathStrategy;
    this.importAsSchema = importAsSchema;

  }

  @Override
  public void importContent(DatabaseExportModule dbExportHandler, SIARDArchiveContainer mainFolder,
    DatabaseStructure databaseStructure, ModuleSettings moduleSettings, ObservableModule observable)
    throws ModuleException {
    this.observable = observable;
    this.databaseStructure = databaseStructure;
    pathStrategy.parseFileIndexMetadata();
    this.dbExportHandler = dbExportHandler;
    Map<Path, SIARDArchiveContainer> archiveContainerByAbsPath = new HashMap<Path, SIARDArchiveContainer>();
    archiveContainerByAbsPath.put(mainFolder.getPath(), mainFolder);
    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    saxParserFactory.setValidating(false); // validation is done using the
                                           // validatorHandler
    saxParserFactory.setNamespaceAware(true);
    SAXParser saxParser = null;
    DigestInputStream xsdInputStream = null;
    SIARDArchiveContainer currentFolder = null;
    assert (databaseStructure.getSchemas().size() == 1);
    this.dbExportHandler.handleDataOpenSchema(importAsSchema);
    long completedSchemas = 0;
    long completedTablesInSchema;
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      completedTablesInSchema = 0;
      observable.notifyOpenSchema(databaseStructure, schema, completedSchemas, completedTablesInSchema);
      assert (schema.getName().equals(importAsSchema));
      for (TableStructure table : schema.getTables()) {
        currentTable = table;
        this.dbExportHandler.handleDataOpenTable(table.getId());
        observable.notifyOpenTable(databaseStructure, table, completedSchemas, completedTablesInSchema);
        rowIndex = 0;
        String xsdFileName = pathStrategy.getTableXSDFilePath(schema.getName(), table.getId());
        String xmlFileName = pathStrategy.getTableXMLFilePath(schema.getName(), table.getId());
        Path archiveFolderLogicalPath = pathStrategy.getArchiveFolderPath(importAsSchema, table.getId());

        Path archiveFolderActualPath = mainFolder.getPath().resolveSibling(archiveFolderLogicalPath);
        if (!archiveContainerByAbsPath.containsKey(archiveFolderActualPath)) {
          archiveContainerByAbsPath.put(archiveFolderActualPath, new SIARDArchiveContainer(archiveFolderActualPath,
            OutputContainerType.MAIN));
        }
        currentFolder = archiveContainerByAbsPath.get(archiveFolderActualPath);
        ValidatorHandler validatorHandler = null;
        try {
          xsdInputStream = readStrategy.createInputStream(currentFolder, xsdFileName,
            pathStrategy.getTableXSDFileMD5(schema.getName(), table.getId()));

          SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
          Schema xmlSchema = factory.newSchema(new StreamSource(xsdInputStream));
          validatorHandler = xmlSchema.newValidatorHandler();
          typeInfoProvider = validatorHandler.getTypeInfoProvider();
          validatorHandler.setContentHandler(this);

          // saxParser.setProperty(JAXP_SCHEMA_LANGUAGE,
          // XMLConstants.W3C_XML_SCHEMA_NS_URI);
          // saxParser.setProperty(JAXP_SCHEMA_SOURCE, xsdInputStream);
        } catch (SAXException e) {
          logger.error("Error validating schema", e);
          throw new ModuleException("Error reading XSD file: "
            + pathStrategy.getTableXSDFilePath(schema.getName(), table.getId()) + " for table:" + table.getId(), e);
        }
        DigestInputStream currentTableInputStream = readStrategy.createInputStream(currentFolder, xmlFileName,
          pathStrategy.getTableXMLFileMD5(schema.getName(), table.getId()));

        SAXErrorHandler saxErrorHandler = new SAXErrorHandler();

        try {
          saxParser = saxParserFactory.newSAXParser();
          XMLReader xmlReader = saxParser.getXMLReader();
          xmlReader.setContentHandler(validatorHandler);
          // xmlReader.setErrorHandler(saxErrorHandler);
          validatorHandler.setErrorHandler(saxErrorHandler);
          logger.debug("begin parse of xml-file:[" + xmlFileName + "], using xsd [" + xsdFileName + "]");
          xmlReader.parse(new InputSource(currentTableInputStream));

        } catch (SAXException e) {
          throw new ModuleException("A SAX error occurred during processing of XML table file for table:"
            + table.getId(), e);
        } catch (IOException e) {
          throw new ModuleException("Error while reading XML table file for table:" + table.getId(), e);
        } catch (ParserConfigurationException e) {
          logger.error("Error creating XML SAXparser", e);
          throw new ModuleException(e);
        }

        if (saxErrorHandler.hasError()) {
          throw new ModuleException("Parsing or validation error occurred while reading XML table file for table:"
            + table.getId());

        }

        readStrategy.closeAndVerifyMD5Sum(currentTableInputStream);
        readStrategy.closeAndVerifyMD5Sum(xsdInputStream);

        completedTablesInSchema++;
        observable.notifyCloseTable(databaseStructure, table, completedSchemas, completedTablesInSchema);
        this.dbExportHandler.handleDataCloseTable(table.getId());
      }
      completedSchemas++;
      observable.notifyCloseSchema(databaseStructure, schema, completedSchemas, schema.getTables().size());
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
          xsdCellType = typeInfoProvider.getElementTypeInfo();
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
        List<Cell> lstCells = Arrays.asList(currentRowCells);
        assert !lstCells.contains(null);
        currentRow.setCells(lstCells);
        try {
          observable.notifyTableProgress(databaseStructure, currentTable, rowIndex - 2, currentTable.getRows());
          this.dbExportHandler.handleDataRow(currentRow);
        } catch (InvalidDataException e) {
          throw new SAXException(e.getMessage() + " Row index:" + rowIndex, e);
        } catch (ModuleException e) {
          throw new SAXException(e.getMessage() + " Row index:" + rowIndex, e);
        }

        isInRowTag = false;
      } else {
        Matcher matcher = XML_ROW_COLUMN_LOCALNAME_PATTERN.matcher(localName);
        if (isInCellTag && matcher.matches()) {
          Integer columnIndex = Integer.valueOf(matcher.group(1));

          Type currentCellType = currentTable.getColumns().get(columnIndex - 1).getType();

          String id = String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex);
          Cell cell;
          String preparedCellVal = currentTagContentStrBld.toString().trim();
          if (currentCellType instanceof SimpleTypeBinary) {
            ModuleException ex = new ModuleException("Siard-dk does not support import of binary values into the db");
            logger.error("Siard-dk does not support the import of binary values into the db", ex);
            throw new SAXException(ex);
          } else {
            if (isInNullValueCell) {
              cell = new NullCell(id);
            } else {
              if (xsdCellType != null && xsdCellType.getTypeName().equalsIgnoreCase("hexBinary")) {
                preparedCellVal = new String(DatatypeConverter.parseHexBinary(preparedCellVal), Charsets.UTF_8);
              } else {
                if (currentCellType instanceof SimpleTypeString) {
                  preparedCellVal = XMLUtils.decode(preparedCellVal);
                }
              }
              cell = new SimpleCell(id, preparedCellVal);
            }
          }
          currentRowCells[columnIndex - 1] = cell;
          isInCellTag = false;
          isInNullValueCell = false;
          xsdCellType = null;
        }

      }
    }

  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    currentTagContentStrBld.append(ch, start, length);
  }

}
