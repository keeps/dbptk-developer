/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.metadata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.model.structure.virtual.VirtualColumnStructure;
import com.databasepreservation.model.structure.virtual.VirtualForeignKey;
import com.databasepreservation.model.structure.virtual.VirtualPrimaryKey;
import com.databasepreservation.model.structure.virtual.VirtualTableStructure;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ArchiveIndex;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ColumnType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ColumnsType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.DocIndexType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ForeignKeyType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ForeignKeysType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.FunctionalDescriptionType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.PrimaryKeyType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ReferenceType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.SiardDiark;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.TableType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.ViewType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128.context.ContextDocumentationIndex;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQL99StandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.metadata.typeConverter.SQLStandardDatatypeImporter;
import com.databasepreservation.modules.siard.in.path.SIARDDK128PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 */
public class SIARDDK128MetadataImportStrategy implements MetadataImportStrategy {

  protected final Logger logger = LoggerFactory.getLogger(SIARDDK128MetadataImportStrategy.class);

  protected final SIARDDK128PathImportStrategy pathStrategy;
  protected DatabaseStructure databaseStructure;
  protected final String importAsSchemaName;
  private int currentTableIndex = 1;

  private SQLStandardDatatypeImporter sqlStandardDatatypeImporter;
  private Reporter reporter;

  public SIARDDK128MetadataImportStrategy(SIARDDK128PathImportStrategy pathStrategy, String importAsSchameName) {
    this.pathStrategy = pathStrategy;
    this.importAsSchemaName = importAsSchameName;
    sqlStandardDatatypeImporter = new SQL99StandardDatatypeImporter();
  }

  @Override
  public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container,
    ModuleConfiguration moduleConfiguration) throws ModuleException {
    FolderReadStrategyMD5Sum readStrategyMD5Sum = null;
    if (!(readStrategy instanceof FolderReadStrategyMD5Sum)) {
      throw new IllegalArgumentException(
        "The current implemenation of SIARDDKMetadataImportStrategy requires relies on the FolderReadStrategyMD5Sum (should be passed to loadMetadata ).");
    }
    readStrategyMD5Sum = (FolderReadStrategyMD5Sum) readStrategy;
    pathStrategy.parseFileIndexMetadata();

    JAXBContext tableIndexContext;
    JAXBContext archiveIndexContext;
    try {
      tableIndexContext = JAXBContext.newInstance(SiardDiark.class.getPackage().getName());
      archiveIndexContext = JAXBContext.newInstance(ArchiveIndex.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema tableIndexXsdSchema = null;
    Schema archiveIndexXsdSchema = null;
    InputStream tableIndexXsdInputStream = readStrategyMD5Sum.createInputStream(container,
      pathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX));
    InputStream archiveIndexXsdInputStream = readStrategyMD5Sum.createInputStream(container,
      pathStrategy.getXsdFilePath(SIARDDKConstants.ARCHIVE_INDEX));
    try {
      tableIndexXsdSchema = schemaFactory.newSchema(new StreamSource(tableIndexXsdInputStream));
      archiveIndexXsdSchema = schemaFactory.newSchema(new StreamSource(archiveIndexXsdInputStream));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.TABLE_INDEX))
        .withCause(e);
    }
    DigestInputStream tableIndexInputStreamXml = null;
    DigestInputStream archiveIndexInputStreamXml = null;
    SiardDiark xmlRoot;
    ArchiveIndex archiveIndex = null;
    Unmarshaller tableIndexUnmarshaller;
    Unmarshaller archiveIndexUnmarshaller;

    try {
      tableIndexUnmarshaller = tableIndexContext.createUnmarshaller();
      tableIndexUnmarshaller.setSchema(tableIndexXsdSchema);
      tableIndexInputStreamXml = readStrategyMD5Sum.createInputStream(container,
        pathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX), pathStrategy.getTabelIndexExpectedMD5Sum());
      xmlRoot = (SiardDiark) tableIndexUnmarshaller.unmarshal(tableIndexInputStreamXml);

      archiveIndexUnmarshaller = archiveIndexContext.createUnmarshaller();
      archiveIndexUnmarshaller.setSchema(archiveIndexXsdSchema);
      if (Files.exists(Paths.get(container.getPath().toString() + SIARDDKConstants.RESOURCE_FILE_SEPARATOR
        + pathStrategy.getXmlFilePath(SIARDDKConstants.ARCHIVE_INDEX)))) {
        archiveIndexInputStreamXml = readStrategyMD5Sum.createInputStream(container,
          pathStrategy.getXmlFilePath(SIARDDKConstants.ARCHIVE_INDEX), pathStrategy.getArchiveIndexExpectedMD5Sum());
        archiveIndex = (ArchiveIndex) archiveIndexUnmarshaller.unmarshal(archiveIndexInputStreamXml);
      }
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Unmarshalling JAXB").withCause(e);
    } finally {
      try {
        tableIndexXsdInputStream.close();
        archiveIndexXsdInputStream.close();

        if (tableIndexInputStreamXml != null) {
          readStrategyMD5Sum.closeAndVerifyMD5Sum(tableIndexInputStreamXml);
        }

        if (archiveIndexInputStreamXml != null) {
          readStrategyMD5Sum.closeAndVerifyMD5Sum(archiveIndexInputStreamXml);
        }
      } catch (IOException e) {
        logger.debug("Could not close xsdStream", e);
      }
    }

    databaseStructure = getDatabaseStructure(xmlRoot, archiveIndex);
  }

  @Override
  public DatabaseStructure getDatabaseStructure() throws ModuleException {
    if (databaseStructure != null) {
      return databaseStructure;
    } else {
      throw new ModuleException().withMessage("getDatabaseStructure must not be called before loadMetadata");
    }
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    sqlStandardDatatypeImporter.setOnceReporter(reporter);
  }

  protected DatabaseStructure getDatabaseStructure(SiardDiark siardArchive, ArchiveIndex archiveIndex)
    throws ModuleException {
    DatabaseStructure databaseStructure = new DatabaseStructure();
    databaseStructure.setProductName(siardArchive.getDatabaseProduct());
    databaseStructure.setSchemas(getSchemas(siardArchive));
    if (archiveIndex != null) {
      setDatabaseMetadata(siardArchive, databaseStructure, archiveIndex);
    } else {
      databaseStructure.setName(siardArchive.getDbName());
    }
    return databaseStructure;
  }

  protected void setDatabaseMetadata(SiardDiark siardArchive, DatabaseStructure databaseStructure,
    ArchiveIndex archiveIndex) {
    databaseStructure.setDbOriginalName(siardArchive.getDbName());
    String[] informationPackageIdSPlit = archiveIndex.getArchiveInformationPackageID().split("\\.");
    String id = informationPackageIdSPlit[informationPackageIdSPlit.length - 1];
    databaseStructure.setName(id + ": " + archiveIndex.getSystemName());
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTime formattedDate = formatter.parseDateTime(archiveIndex.getArchivePeriodEnd());
    databaseStructure.setArchivalDate(formattedDate);
    databaseStructure.setDataOriginTimespan(archiveIndex.getArchivePeriodStart()
      + SIARDDKConstants.RESOURCE_FILE_SEPARATOR + archiveIndex.getArchivePeriodEnd());
    StringBuilder creatorsList = new StringBuilder();
    List<JAXBElement<String>> creatorListElements = archiveIndex.getArchiveCreatorList()
      .getCreatorNameAndCreationPeriodStartAndCreationPeriodEnd();
    for (int i = 0; i < creatorListElements.size(); i++) {
      JAXBElement<String> element = creatorListElements.get(i);
      if (element.getName().getLocalPart().equals(SIARDDKConstants.CREATOR_NAME)) {
        creatorsList.append(element.getValue());

        boolean isLastCreatorName = true;
        for (int j = i + 1; j < creatorListElements.size(); j++) {
          if (creatorListElements.get(j).getName().getLocalPart().equals(SIARDDKConstants.CREATOR_NAME)) {
            isLastCreatorName = false;
            break;
          }
        }

        if (!isLastCreatorName) {
          creatorsList.append("; ");
        }
      }
    }

    databaseStructure.setDataOwner(creatorsList.toString());
    databaseStructure.setDescription(archiveIndex.getSystemPurpose());
  }

  protected List<SchemaStructure> getSchemas(SiardDiark siardArchive) throws ModuleException {
    SchemaStructure schemaImportAs = new SchemaStructure();
    schemaImportAs.setName(getImportAsSchemaName());
    schemaImportAs.setTables(getTables(siardArchive));
    schemaImportAs.setViews(getViews(siardArchive));
    List<SchemaStructure> list = new LinkedList<SchemaStructure>();
    list.add(schemaImportAs);

    return list;

  }

  protected List<ViewStructure> getViews(SiardDiark siardArchive) {
    List<ViewStructure> lstViewsDptkl = new LinkedList<ViewStructure>();
    if (siardArchive.getViews() != null && siardArchive.getViews().getView() != null) {
      for (ViewType viewXml : siardArchive.getViews().getView()) {
        ViewStructure viewDptkl = new ViewStructure();
        if (StringUtils.isNotBlank(viewXml.getDescription())) {
          viewDptkl.setDescription(viewXml.getDescription());
        }
        viewDptkl.setName(viewXml.getName());
        viewDptkl.setQueryOriginal(viewXml.getQueryOriginal());
        // NOTICE: As siard-dk only support defining the query original
        // attribute -
        // we'll use it for both the query and the query original field in the
        // internal representation of the view.
        viewDptkl.setQuery(viewXml.getQueryOriginal());
        lstViewsDptkl.add(viewDptkl);
      }
    }
    return lstViewsDptkl;
  }

  protected List<TableStructure> getTables(SiardDiark siardArchive) throws ModuleException {
    List<TableStructure> lstTblsDptkl = new LinkedList<TableStructure>();
    if (siardArchive.getTables() != null && siardArchive.getTables().getTable() != null) {
      boolean needsVirtualTable = false;
      for (TableType tblXml : siardArchive.getTables().getTable()) {
        TableStructure tblDptkl = new TableStructure();
        tblDptkl.setIndex(currentTableIndex++);
        tblDptkl.setSchema(getImportAsSchemaName());
        tblDptkl.setName(tblXml.getName());
        tblDptkl.setId(String.format("%s.%s", tblDptkl.getSchema(), tblDptkl.getName()));
        tblDptkl.setDescription(tblXml.getDescription());
        tblDptkl.setPrimaryKey(getPrimaryKey(tblXml.getPrimaryKey()));
        tblDptkl.setForeignKeys(getForeignKeys(tblXml.getForeignKeys(), tblDptkl.getId()));
        tblDptkl.setRows(getNumberOfTblRows(tblXml.getRows(), tblXml.getName()));
        tblDptkl.setColumns(getTblColumns(tblXml.getColumns(), tblDptkl.getId()));
        List<ForeignKey> virtualForeignKeys = getVirtualForeignKeys(tblXml.getColumns(), tblDptkl.getId());
        if (!virtualForeignKeys.isEmpty()) {
          tblDptkl.getForeignKeys().addAll(virtualForeignKeys);
          needsVirtualTable = true;
        }
        pathStrategy.associateTableWithFolder(tblDptkl.getId(), tblXml.getFolder());
        lstTblsDptkl.add(tblDptkl);
      }
      if (needsVirtualTable) {
        lstTblsDptkl.add(createVirtualTable());
      }
      TableStructure contextDocumentationTable = createContextDocumentationTable();
      if (contextDocumentationTable != null) {
        lstTblsDptkl.add(contextDocumentationTable);
      }
    }
    return lstTblsDptkl;
  }

  private TableStructure createContextDocumentationTable() throws ModuleException {
    try {
      ContextDocumentationIndex contextDocumentationIndex = loadContextDocumentationTableMetadata();
      if (contextDocumentationIndex == null) {
        return null;
      } else {
        VirtualTableStructure virtualTable = new VirtualTableStructure();
        virtualTable.setIndex(currentTableIndex++);
        virtualTable.setSchema(getImportAsSchemaName());
        virtualTable.setId(
          String.format("%s.%s", virtualTable.getSchema(), SIARDDKConstants.CONTEXT_DOCUMENTATION_VIRTUAL_TABLE_NAME));
        virtualTable.setName(SIARDDKConstants.CONTEXT_DOCUMENTATION_VIRTUAL_TABLE_NAME);
        virtualTable.setDescription(SIARDDKConstants.CONTEXT_DOCUMENTATION_VIRTUAL_TABLE_DESCRIPTION);
        virtualTable.setRows(contextDocumentationIndex.getDocument().size());
        virtualTable.setColumns(createContextDocumentsTableColumns());
        virtualTable.setPrimaryKey(createVirtualPrimaryKey(
          SIARDDKConstants.CONTEXT_DOCUMENTATION_VIRTUAL_TABLE_PRIMARY_KEY_NAME, SIARDDKConstants.DID));
        return virtualTable;
      }
    } catch (FileNotFoundException e) {
      throw new ModuleException().withMessage(
        "Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX))
        .withCause(e);
    }
  }

  private ContextDocumentationIndex loadContextDocumentationTableMetadata()
    throws ModuleException, FileNotFoundException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(ContextDocumentationIndex.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }
    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdInputStream = new FileInputStream(
      pathStrategy.getMainFolder().getPath().toString() + SIARDDKConstants.RESOURCE_FILE_SEPARATOR
        + pathStrategy.getXsdFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdInputStream));
    } catch (SAXException e) {
      throw new ModuleException().withMessage(
        "Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX))
        .withCause(e);
    }
    InputStream inputStreamXml = null;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);
      String contextDocumentationIndexFilePath = pathStrategy.getMainFolder().getPath().toString()
        + SIARDDKConstants.RESOURCE_FILE_SEPARATOR
        + pathStrategy.getXmlFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX);
      if (Paths.get(contextDocumentationIndexFilePath).toFile().exists()) {
        inputStreamXml = new FileInputStream(contextDocumentationIndexFilePath);
        ContextDocumentationIndex jaxbElement = (ContextDocumentationIndex) unmarshaller.unmarshal(inputStreamXml);
        return jaxbElement;
      } else {
        return null;
      }
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

  private DocIndexType loadVirtualTableMetadata() throws ModuleException, FileNotFoundException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(DocIndexType.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdInputStream = new FileInputStream(pathStrategy.getMainFolder().getPath().toString()
      + SIARDDKConstants.RESOURCE_FILE_SEPARATOR + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX));

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
      inputStreamXml = new FileInputStream(pathStrategy.getMainFolder().getPath().toString()
        + SIARDDKConstants.RESOURCE_FILE_SEPARATOR + pathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX));
      Object result = unmarshaller.unmarshal(inputStreamXml);
      DocIndexType docIndex;
      if (result instanceof JAXBElement) {
        docIndex = ((JAXBElement<DocIndexType>) result).getValue();
      } else if (result instanceof DocIndexType) {
        docIndex = (DocIndexType) result;
      } else {
        throw new IllegalArgumentException("Unexpected object type: " + result.getClass().getName());
      }
      return docIndex;
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

  private List<ForeignKey> getVirtualForeignKeys(ColumnsType columns, String tableId) {
    List<ForeignKey> virtualForeignKeys = new ArrayList<>();
    for (ColumnType column : columns.getColumn()) {
      if (column.getFunctionalDescription() != null
        && column.getFunctionalDescription().contains(FunctionalDescriptionType.DOKUMENTIDENTIFIKATION)) {
        VirtualForeignKey virtualForeignKey = new VirtualForeignKey();
        virtualForeignKey.setReferencedSchema(getImportAsSchemaName());
        virtualForeignKey.setName(SIARDDKConstants.VIRTUAL_TABLE_FOREIGN_KEY_NAME);
        virtualForeignKey.setReferencedTable(SIARDDKConstants.VIRTUAL_TABLE_NAME);
        Reference reference = new Reference(column.getName(), SIARDDKConstants.DID);
        List<Reference> referenceList = new ArrayList<>();
        referenceList.add(reference);
        virtualForeignKey.setReferences(referenceList);
        virtualForeignKey.setId(String.format("%s.%s", tableId, SIARDDKConstants.VIRTUAL_TABLE_FOREIGN_KEY_NAME));
        virtualForeignKeys.add(virtualForeignKey);
      }
    }

    return virtualForeignKeys;
  }

  private TableStructure createVirtualTable() throws ModuleException {
    try {
      DocIndexType docIndexType = loadVirtualTableMetadata();
      VirtualTableStructure virtualTable = new VirtualTableStructure();
      virtualTable.setIndex(currentTableIndex++);
      virtualTable.setSchema(getImportAsSchemaName());
      virtualTable.setId(String.format("%s.%s", virtualTable.getSchema(), SIARDDKConstants.VIRTUAL_TABLE_NAME));
      virtualTable.setName(SIARDDKConstants.VIRTUAL_TABLE_NAME);
      virtualTable.setDescription(SIARDDKConstants.VIRTUAL_TABLE_DESCRIPTION);
      virtualTable.setRows(docIndexType.getDoc().size());
      virtualTable.setColumns(createVirtualTableColumns());
      virtualTable
        .setPrimaryKey(createVirtualPrimaryKey(SIARDDKConstants.VIRTUAL_TABLE_PRIMARY_KEY_NAME, SIARDDKConstants.DID));
      return virtualTable;
    } catch (FileNotFoundException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX))
        .withCause(e);
    }
  }

  private List<ColumnStructure> createContextDocumentsTableColumns() {
    List<ColumnStructure> columnStructureList = new ArrayList<>();
    Type typeInt = sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
      "<information unavailable>", "<information unavailable>", "INTEGER", "INTEGER");
    Type typeChar = sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
      "<information unavailable>", "<information unavailable>", "CHARACTER", "CHARACTER");
    Type typeBlob = sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
      "<information unavailable>", "<information unavailable>", Constants.BINARY_LARGE_OBJECT,
      Constants.BINARY_LARGE_OBJECT);
    VirtualColumnStructure columnID = new VirtualColumnStructure(SIARDDKConstants.DOCUMENT_ID,
      SIARDDKConstants.DOCUMENT_ID, typeInt, true, SIARDDKConstants.DOCUMENT_IDENTIFIER, "1", true);
    VirtualColumnStructure columnTitle = new VirtualColumnStructure(SIARDDKConstants.DOCUMENT_TITLE,
      SIARDDKConstants.DOCUMENT_TITLE, typeChar, true, SIARDDKConstants.DOCUMENT_TITLE_DESCRIPTION, "", true);
    VirtualColumnStructure columnDate = new VirtualColumnStructure(SIARDDKConstants.DOCUMENT_DATE,
      SIARDDKConstants.DOCUMENT_DATE, typeChar, true, SIARDDKConstants.DOCUMENT_DATE_DESCRIPTION, "", true);
    VirtualColumnStructure columnLOB = new VirtualColumnStructure(Constants.BLOB, Constants.BLOB_COLUMN_NAME, typeBlob,
      true, "", "1", true);
    columnStructureList.add(columnID);
    columnStructureList.add(columnTitle);
    columnStructureList.add(columnDate);
    columnStructureList.add(columnLOB);
    return columnStructureList;
  }

  private List<ColumnStructure> createVirtualTableColumns() {
    List<ColumnStructure> columnStructureList = new ArrayList<>();
    Type typeInt = sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
      "<information unavailable>", "<information unavailable>", "INTEGER", "INTEGER");
    VirtualColumnStructure columnID = new VirtualColumnStructure(SIARDDKConstants.DID, SIARDDKConstants.DID, typeInt,
      true, SIARDDKConstants.DOCUMENT_IDENTIFIER, "1", true);
    Type type = sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
      "<information unavailable>", "<information unavailable>", Constants.BINARY_LARGE_OBJECT,
      Constants.BINARY_LARGE_OBJECT);
    VirtualColumnStructure columnLOB = new VirtualColumnStructure(Constants.BLOB, Constants.BLOB_COLUMN_NAME, type,
      true, "", "1", true);
    columnStructureList.add(columnID);
    columnStructureList.add(columnLOB);
    return columnStructureList;
  }

  private PrimaryKey createVirtualPrimaryKey(String name, String columnName) {
    List<String> columnList = new ArrayList<>();
    columnList.add(columnName);
    return new VirtualPrimaryKey(name, columnList, SIARDDKConstants.VIRTUAL_TABLE_PRIMARY_KEY_DESCRIPTION);
  }

  protected List<ColumnStructure> getTblColumns(ColumnsType columnsXml, String tableId) throws ModuleException {
    List<ColumnStructure> lstColumnsDptkl = new LinkedList<ColumnStructure>();
    if (columnsXml != null && columnsXml.getColumn() != null) {
      for (ColumnType columnXml : columnsXml.getColumn()) {
        ColumnStructure columnDptkl = new ColumnStructure();
        columnDptkl.setName(columnXml.getName());
        columnDptkl.setId(String.format("%s.%s", tableId, columnDptkl.getName()));
        String typeOriginal = StringUtils.isNotBlank(columnXml.getTypeOriginal()) ? columnXml.getTypeOriginal() : null;
        columnDptkl
          .setType(sqlStandardDatatypeImporter.getCheckedType("<information unavailable>", "<information unavailable>",
            "<information unavailable>", "<information unavailable>", columnXml.getType(), typeOriginal));
        columnDptkl.setDescription(columnXml.getDescription());
        String defaultValue = StringUtils.isNotBlank(columnXml.getDefaultValue()) ? columnXml.getDefaultValue() : null;
        columnDptkl.setDefaultValue(defaultValue);
        columnDptkl.setNillable(columnXml.isNullable());
        lstColumnsDptkl.add(columnDptkl);
      }
    }
    return lstColumnsDptkl;
  }

  protected Type getType(String type) {

    return null;
  }

  protected long getNumberOfTblRows(BigInteger numRows, String tableName) throws ModuleException {
    try {
      return numRows.longValue();
    } catch (ArithmeticException e) {
      throw new ModuleException().withMessage("Unable to import table [" + tableName + "], as the number of rows ["
        + numRows
        + "] exceeds the max value of the long datatype used to store the number.(Consult the vendor/a programmer for a fix of this problem, if needed)")
        .withCause(e);
    }
  }

  protected PrimaryKey getPrimaryKey(PrimaryKeyType primaryKeyXml) {
    PrimaryKey keyDptkl = new PrimaryKey();
    keyDptkl.setName(primaryKeyXml.getName());
    keyDptkl.setColumnNames(primaryKeyXml.getColumn());
    return keyDptkl;
  }

  protected List<ForeignKey> getForeignKeys(ForeignKeysType foreignKeysXml, String tableId) {
    List<ForeignKey> lstForeignKeyDptkl = new LinkedList<ForeignKey>();
    if (foreignKeysXml != null) {
      for (ForeignKeyType foreignKeyXml : foreignKeysXml.getForeignKey()) {
        ForeignKey foreignKeyDptkl = new ForeignKey();
        foreignKeyDptkl.setReferencedSchema(getImportAsSchemaName());
        foreignKeyDptkl.setName(foreignKeyXml.getName());
        foreignKeyDptkl.setReferencedTable(foreignKeyXml.getReferencedTable());
        foreignKeyDptkl.setReferences(getReferences(foreignKeyXml.getReference()));
        foreignKeyDptkl.setId(String.format("%s.%s", tableId, foreignKeyDptkl.getName()));
        lstForeignKeyDptkl.add(foreignKeyDptkl);
      }
    }
    return lstForeignKeyDptkl;
  }

  protected List<Reference> getReferences(List<ReferenceType> referencesXml) {
    List<Reference> refsDptkld = new LinkedList<Reference>();
    if (referencesXml != null) {
      for (ReferenceType referenceTypeXml : referencesXml) {
        Reference refDptkld = new Reference();
        refDptkld.setColumn(referenceTypeXml.getColumn());
        refDptkld.setReferenced(referenceTypeXml.getReferenced());
        refsDptkld.add(refDptkld);
      }
    }
    return refsDptkld;
  }

  public String getImportAsSchemaName() {
    return importAsSchemaName;
  }
}