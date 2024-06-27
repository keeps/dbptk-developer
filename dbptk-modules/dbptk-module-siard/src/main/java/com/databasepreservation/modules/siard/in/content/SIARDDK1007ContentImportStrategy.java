/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.content;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;

import com.databasepreservation.common.io.providers.DummyInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import dk.sa.xmlns.diark._1_0.docindex.DocIndexType;
import dk.sa.xmlns.diark._1_0.docindex.DocumentType;
import jakarta.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.TypeInfoProvider;
import javax.xml.validation.ValidatorHandler;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.TypeInfo;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer.OutputContainerType;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.path.SIARDDK1007PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.utils.XMLUtils;

public class SIARDDK1007ContentImportStrategy extends DefaultHandler implements ContentImportStrategy {
  private static final Logger logger = LoggerFactory.getLogger(SIARDDK1007ContentImportStrategy.class);
  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDDK1007PathImportStrategy pathStrategy;
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
  private DatabaseStructure databaseStructure;

  /**
   * @author Thomas Kristensen <tk@bithuset.dk>
   *
   */
  public SIARDDK1007ContentImportStrategy(FolderReadStrategyMD5Sum readStrategy, SIARDDK1007PathImportStrategy pathStrategy,
                                          String importAsSchema) {
    this.readStrategy = readStrategy;
    this.pathStrategy = pathStrategy;
    this.importAsSchema = importAsSchema;

  }

  @Override
  public void importContent(DatabaseExportModule dbExportHandler, SIARDArchiveContainer mainFolder,
                            DatabaseStructure databaseStructure, ModuleConfiguration moduleConfiguration) throws ModuleException {
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
      assert (schema.getName().equals(importAsSchema));
      for (TableStructure table : schema.getTables()) {
        if (!table.getId().split("\\.")[1].equals("virtual_table")) {
          currentTable = table;
          this.dbExportHandler.handleDataOpenTable(table.getId());
          rowIndex = 0;
          String xsdFileName = pathStrategy.getTableXSDFilePath(schema.getName(), table.getId());
          String xmlFileName = pathStrategy.getTableXMLFilePath(schema.getName(), table.getId());
          Path archiveFolderLogicalPath = pathStrategy.getArchiveFolderPath(importAsSchema, table.getId());

          Path archiveFolderActualPath = mainFolder.getPath().resolveSibling(archiveFolderLogicalPath);
          if (!archiveContainerByAbsPath.containsKey(archiveFolderActualPath)) {
            archiveContainerByAbsPath.put(archiveFolderActualPath, new SIARDArchiveContainer(
              SIARDConstants.SiardVersion.DK, archiveFolderActualPath, OutputContainerType.MAIN));
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
            throw new ModuleException().withMessage("Error reading XSD file: "
                + pathStrategy.getTableXSDFilePath(schema.getName(), table.getId()) + " for table:" + table.getId())
              .withCause(e);
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
            throw new ModuleException()
              .withMessage("A SAX error occurred during processing of XML table file for table:" + table.getId())
              .withCause(e);
          } catch (IOException e) {
            throw new ModuleException().withMessage("Error while reading XML table file for table:" + table.getId())
              .withCause(e);
          } catch (ParserConfigurationException e) {
            logger.error("Error creating XML SAXparser", e);
            throw new ModuleException().withCause(e);
          }

          if (saxErrorHandler.hasError()) {
            throw new ModuleException().withMessage(
              "Parsing or validation error occurred while reading XML table file for table:" + table.getId());

          }

          readStrategy.closeAndVerifyMD5Sum(currentTableInputStream);
          readStrategy.closeAndVerifyMD5Sum(xsdInputStream);

          completedTablesInSchema++;
          this.dbExportHandler.handleDataCloseTable(table.getId());
        } else {
          try {
            DocIndexType docIndex = loadVirtualTableContent();
            populateVirtualTable(docIndex, table);
          } catch (FileNotFoundException e) {
            throw new ModuleException()
              .withMessage("Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX))
              .withCause(e);
          }
        }
      }
      completedSchemas++;
      this.dbExportHandler.handleDataCloseSchema(importAsSchema);
    }

  }

  private void populateVirtualTable(DocIndexType docIndex, TableStructure table) throws ModuleException {
    currentTable = table;
    this.dbExportHandler.handleDataOpenTable(table.getId());
    Cell[] cells = new Cell[table.getColumns().size()];
    for (DocumentType doc : docIndex.getDoc()) {
      int rowCounter = 0;
      int cellCounter = 0;
      Row row = new Row();
      row.setIndex(rowCounter);
      Cell dIDCell = new SimpleCell("dID." + rowCounter, doc.getDID().toString());
      cells[cellCounter] = dIDCell;
      cellCounter++;
      String binPath = pathStrategy.getMainFolder().getPath().toString() + "/Documents/" + doc.getDCf() + "/" + doc.getDID() + "/"
        + doc.getMID() + "." + doc.getAFt();
      try {
        Cell blobCell = new BinaryCell("blob." + rowCounter, new DummyInputStreamProvider(), binPath, 0L, DigestUtils.sha1Hex(FileUtils.readFileToByteArray(Paths.get(binPath).toFile())), DigestUtils.getSha1Digest().toString());
        cells[cellCounter] = blobCell;
        cellCounter++;
        rowCounter++;
        List<Cell> lstCells = Arrays.asList(cells);
        assert !lstCells.contains(null);
        row.setCells(lstCells);
        this.dbExportHandler.handleDataRow(row);
      } catch (ModuleException | IOException e) {
        throw new ModuleException().withMessage("Error handling data row index:" + rowCounter).withCause(e);
      }
    }
    this.dbExportHandler.handleDataCloseTable(table.getId());
  }

  private DocIndexType loadVirtualTableContent() throws ModuleException, FileNotFoundException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(DocIndexType.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdInputStream = new FileInputStream(pathStrategy.getMainFolder().getPath().toString() + "/"
      + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX));

    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdInputStream));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX))
        .withCause(e);
    }
    InputStream inputStreamXml = null;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);
      inputStreamXml = new FileInputStream(pathStrategy.getMainFolder().getPath().toString() + "/" +
        pathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX));
      JAXBElement<DocIndexType> jaxbElement = (JAXBElement<DocIndexType>) unmarshaller.unmarshal(inputStreamXml);
      return jaxbElement.getValue();
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Unmarshalling JAXB").withCause(e);
    } finally {
      try {
        xsdInputStream.close();
        if (inputStreamXml != null) {
          inputStreamXml.close();
          xsdInputStream.close();
        }
      } catch (IOException e) {
        logger.debug("Could not close xsdStream", e);
      }
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
            ModuleException ex = new ModuleException()
              .withMessage("Siard-dk does not support import of binary values into the db");
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
