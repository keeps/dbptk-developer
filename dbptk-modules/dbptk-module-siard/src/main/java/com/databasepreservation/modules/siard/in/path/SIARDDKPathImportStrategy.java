/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.ValidatorHandler;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.utils.ConfigUtils;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 *         NOTICE: The SIARDDKPathImportStrategy implements both the
 *         ContentPathImportStrategy and the MetadataPathStrategy. Both are
 *         consolidated in one file, as both rely on parsing the fileIndex.xml,
 *         to retrieve md5sums.(The impl. of retrieval of md5sums for the meta
 *         data files are only implemented to the extend that it is needed. )
 */
public abstract class SIARDDKPathImportStrategy<T, D> extends DefaultHandler
  implements ContentPathImportStrategy, MetadataPathStrategy {
  protected final Logger logger = LoggerFactory.getLogger(ContentPathImportStrategy.class);
  protected final String importAsSchema;
  protected final SIARDArchiveContainer mainFolder;
  protected final ReadStrategy readStrategy;
  protected final MetadataPathStrategy metadataPathStrategy;
  protected final DB mapDB;
  protected final Map<String, T> xmlFilePathLookupByFolderName;
  protected final Map<String, T> xsdFilePathLookupByFolderName;
  protected final Map<String, String> folderNameLookupByTableId;
  protected final Map<String, Path> archiveFolderLookupByFolderName;
  protected final Pattern folderSperatorPattern = Pattern.compile("[\\\\\\/]");
  private final Class<D> fileIndexTypeClass;
  // protected byte[] fileIndexExpectedMD5Sum; --For some reason, no md5sum is
  // required for fileIndex.xml in the standard
  protected byte[] tableIndexExpectedMD5Sum;
  protected byte[] archiveIndexExpectedMD5Sum;
  protected boolean fileIndexIsParsed;
  private FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy;

  public SIARDDKPathImportStrategy(SIARDArchiveContainer mainFolder, ReadStrategy readStrategy,
    MetadataPathStrategy metadataPathStrategy, String importAsSchema,
    FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy, Class<D> fileIndexTypeClass) {
    super();
    this.mainFolder = mainFolder;
    this.readStrategy = readStrategy;
    this.metadataPathStrategy = metadataPathStrategy;
    this.importAsSchema = importAsSchema;
    this.fileIndexXsdInputStreamStrategy = fileIndexXsdInputStreamStrategy;
    this.fileIndexTypeClass = fileIndexTypeClass;

    // Lookups maps
    this.mapDB = setupMapDB();
    this.xmlFilePathLookupByFolderName = this.mapDB
      .hashMap("xmlFilePathLookupByFolderName", Serializer.STRING, Serializer.JAVA).createOrOpen();
    this.xsdFilePathLookupByFolderName = this.mapDB
      .hashMap("xsdFilePathLookupByFolderName", Serializer.STRING, Serializer.JAVA).createOrOpen();
    this.folderNameLookupByTableId = this.mapDB
      .hashMap("xsdFilePathLookupByFolderName", Serializer.STRING, Serializer.STRING).createOrOpen();
    this.archiveFolderLookupByFolderName = this.mapDB
      .hashMap("archiveFolderLookupByFolderName", Serializer.STRING, Serializer.JAVA).createOrOpen();
  }

  public void parseFileIndexMetadata() throws ModuleException {

    if (!fileIndexIsParsed) {
      JAXBContext context;
      try {
        context = JAXBContext.newInstance(fileIndexTypeClass.getPackage().getName());
      } catch (JAXBException e) {
        throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
      }

      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema xsdSchema = null;
      InputStream xsdStream = fileIndexXsdInputStreamStrategy.getInputStream(this);
      ValidatorHandler validatorHandler = null;
      SIARDDKFileIndexHandler<T> fileIndexHandler = null;
      try {
        xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
        validatorHandler = xsdSchema.newValidatorHandler();
        fileIndexHandler = createFileIndexHandler();
        validatorHandler.setContentHandler(fileIndexHandler);
      } catch (SAXException e) {
        throw new ModuleException()
          .withMessage(
            "Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX))
          .withCause(e);
      }

      try {
        SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        saxParserFactory.setValidating(false);
        saxParserFactory.setNamespaceAware(true);
        SAXParser saxParser = saxParserFactory.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(validatorHandler);
        xmlReader.parse(new InputSource(readStrategy.createInputStream(mainFolder,
          metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX))));
      } catch (SAXException | ParserConfigurationException | IOException e) {
        throw new ModuleException().withMessage("Error while parsing file index").withCause(e);
      } finally {
        try {
          xsdStream.close();
        } catch (IOException e) {
          logger.debug("Could not close xsdStream", e);
        }
      }

      tableIndexExpectedMD5Sum = fileIndexHandler.getTableIndexExpectedMD5Sum();
      archiveIndexExpectedMD5Sum = fileIndexHandler.getArchiveIndexExpectedMD5Sum();

      fileIndexIsParsed = true;
    }
  }

  private DB setupMapDB() {
    Path fileDBPath;
    String fileDirectoryLocation = ConfigUtils.getProperty(Constants.PROPERTY_UNSET, "dbptk.memory.dir");
    if (fileDirectoryLocation.equals(Constants.PROPERTY_UNSET)) {
      fileDBPath = Paths.get(ConfigUtils.getMapDBHomeDirectory().normalize().toAbsolutePath().toString(),
        UUID.randomUUID().toString());
    } else {
      fileDBPath = Paths.get(fileDirectoryLocation, UUID.randomUUID().toString());
    }
    return DBMaker.fileDB(fileDBPath.toFile()).fileDeleteAfterClose().fileMmapEnable().fileMmapEnableIfSupported()
      .fileMmapPreclearDisable().closeOnJvmShutdown().make();
  }

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
    throw new UnsupportedOperationException("Invoking getLobPath(...) is not relevant for SIARDDK.");
  }

  @Override
  public String getLobPathFallback(String basePath, String columnId, String lobFileName) {
    throw new UnsupportedOperationException("Invoking getLobPath(...) is not relevant for SIARDDK.");
  }

  @Override
  public void associateSchemaWithFolder(String schemaName, String schemaFolder) {
    throw new UnsupportedOperationException("Invoking associateSchemaWithFolder(...) is not relevant for SIARDDK.");
  }

  @Override
  public void associateTableWithFolder(String tableId, String tableFolder) {
    folderNameLookupByTableId.put(tableId, tableFolder);
  }

  @Override
  public void associateColumnWithFolder(String columnId, String columnFolder) {
    throw new UnsupportedOperationException("Invoking associateColumnWithFolder(...) is not relevant for SIARDDK.");

  }

  protected void canLookupTable(String schemaName, String tableId) throws ModuleException {
    if (!schemaName.equals(schemaName)) {
      throw new ModuleException().withMessage("SIARDDK does not support multiple schemas. The given schema ["
        + schemaName + "] is not identical to the schema name given on start up: [" + this.importAsSchema + "]");
    }

    if (!folderNameLookupByTableId.containsKey(tableId)) {
      throw new ModuleException().withMessage(
        "No folder name has - during the parsing of the database sctructure - been associated with the given table id:"
          + tableId);
    }
  }

  protected void canLookupXMLFilePath(String folderName) throws ModuleException {
    if (!xmlFilePathLookupByFolderName.containsKey(folderName)) {
      throw new ModuleException().withMessage(
        "No xml file path has - during the parsing of the file index - been associated with the folder name:"
          + folderName);
    }
  }

  @Override
  public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
    return buildPathSansArchiveFolderName(getTableXMLFileInfo(schemaName, tableId));
  }

  public byte[] getTableXMLFileMD5(String schemaName, String tableId) throws ModuleException {
    return getMd5(getTableXMLFileInfo(schemaName, tableId));
  }

  public byte[] getArchiveIndexExpectedMD5Sum() throws ModuleException {
    if (archiveIndexExpectedMD5Sum == null && fileIndexIsParsed) {
      throw new ModuleException()
        .withMessage("Parsing of " + SIARDDKConstants.FILE_INDEX + "." + SIARDDKConstants.XML_EXTENSION
          + " did not provide a md5sum for " + SIARDDKConstants.ARCHIVE_INDEX + "." + SIARDDKConstants.XML_EXTENSION);
    }
    return archiveIndexExpectedMD5Sum;
  }

  protected void canLookupXSDFilePath(String folderName) throws ModuleException {
    if (!xsdFilePathLookupByFolderName.containsKey(folderName)) {
      throw new ModuleException().withMessage(
        "No xsd file path has - during the parsing of the file index - been associated with the folder name:"
          + folderName);
    }
  }

  protected T getTableXMLFileInfo(String schemaName, String tableId) throws ModuleException {
    canLookupTable(schemaName, tableId);
    String folderName = folderNameLookupByTableId.get(tableId);
    canLookupXMLFilePath(folderName);
    return xmlFilePathLookupByFolderName.get(folderName);
  }

  protected T getTableXSDFileInfo(String schemaName, String tableId) throws ModuleException {
    canLookupTable(schemaName, tableId);
    String folderName = folderNameLookupByTableId.get(tableId);
    canLookupXSDFilePath(folderName);
    return xsdFilePathLookupByFolderName.get(folderName);
  }

  protected String buildPathSansArchiveFolderName(T fileInfo) {
    Path pathFolderSperatorNeutral = FileSystems.getDefault().getPath("",
      folderSperatorPattern.split(getFoN(fileInfo)));
    pathFolderSperatorNeutral = pathFolderSperatorNeutral.subpath(1, pathFolderSperatorNeutral.getNameCount());
    Path pathFolderSperatorNeutralWithFile = pathFolderSperatorNeutral.resolve(getFiN(fileInfo));
    return pathFolderSperatorNeutralWithFile.toString();
  }

  @Override
  public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
    return buildPathSansArchiveFolderName(getTableXSDFileInfo(schemaName, tableId));
  }

  public byte[] getTableXSDFileMD5(String schemaName, String tableId) throws ModuleException {
    return getMd5(getTableXSDFileInfo(schemaName, tableId));
  }

  public Path getArchiveFolderPath(String schemaName, String tableId) throws ModuleException {
    canLookupTable(schemaName, tableId);
    String folderName = folderNameLookupByTableId.get(tableId);
    assert (archiveFolderLookupByFolderName.containsKey(folderName));
    return archiveFolderLookupByFolderName.get(folderName);
  }

  @Override
  public String getXmlFilePath(String filename) throws InvalidParameterException {
    return metadataPathStrategy.getXmlFilePath(filename);
  }

  @Override
  public String getXsdFilePath(String filename) throws InvalidParameterException {
    return metadataPathStrategy.getXsdFilePath(filename);
  }

  @Override
  public String getXsdResourcePath(String filename) throws InvalidParameterException {
    return metadataPathStrategy.getXsdResourcePath(filename);
  }

  /*
   * public byte[] getFileIndexExpectedMD5Sum() throws ModuleException { if
   * (fileIndexExpectedMD5Sum == null && fileIndexIsParsed) { throw new
   * ModuleException("Parsing of " + SIARDDKConstants.FILE_INDEX + "." +
   * SIARDDKConstants.XML_EXTENSION + " did not provide a md5sum for " +
   * SIARDDKConstants.FILE_INDEX + "." + SIARDDKConstants.XML_EXTENSION); } return
   * fileIndexExpectedMD5Sum; }
   */

  public byte[] getTableIndexExpectedMD5Sum() throws ModuleException {
    if (tableIndexExpectedMD5Sum == null && fileIndexIsParsed) {
      throw new ModuleException()
        .withMessage("Parsing of " + SIARDDKConstants.FILE_INDEX + "." + SIARDDKConstants.XML_EXTENSION
          + " did not provide a md5sum for " + SIARDDKConstants.TABLE_INDEX + "." + SIARDDKConstants.XML_EXTENSION);
    }
    return tableIndexExpectedMD5Sum;
  }

  /**
   * @return the readStrategy
   */
  public ReadStrategy getReadStrategy() {
    return readStrategy;
  }

  /**
   * @return the mainFolder
   */
  public SIARDArchiveContainer getMainFolder() {
    return mainFolder;
  }

  abstract SIARDDKFileIndexHandler<T> createFileIndexHandler();

  abstract byte[] getMd5(T fileInfo);

  abstract List<T> getF(D fileIndex);

  abstract String getFoN(T fileInfo);

  abstract String getFiN(T fileInfo);
}
