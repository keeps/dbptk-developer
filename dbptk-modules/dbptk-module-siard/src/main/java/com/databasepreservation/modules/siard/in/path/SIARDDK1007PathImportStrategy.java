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
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;

import com.databasepreservation.modules.siard.bindings.siard_dk_1007.FileIndexType;
import com.databasepreservation.modules.siard.bindings.siard_dk_1007.FileIndexType.F;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 * 
 *         NOTICE: The SIARDDKPathImportStrategy implements both the
 *         ContentPathImportStrategy and the MetadataPathStrategy. Both are
 *         consolidated in one file, as both rely on parsing the fileIndex.xml,
 *         to retrieve md5sums.(The impl. of retrieval of md5sums for the meta
 *         data files are only implemented to the extend that it is needed. )
 */
public class SIARDDK1007PathImportStrategy implements ContentPathImportStrategy, MetadataPathStrategy {
  protected final Logger logger = LoggerFactory.getLogger(ContentPathImportStrategy.class);
  protected final String importAsSchema;
  protected final SIARDArchiveContainer mainFolder;
  protected final ReadStrategy readStrategy;
  protected final MetadataPathStrategy metadataPathStrategy;
  protected final Map<String, FileIndexType.F> xmlFilePathLookupByFolderName = new HashMap<String, FileIndexType.F>();
  protected final Map<String, FileIndexType.F> xsdFilePathLookupByFolderName = new HashMap<String, FileIndexType.F>();
  protected final Map<String, String> folderNameLookupByTableId = new HashMap<String, String>();
  protected final Map<String, Path> archiveFolderLookupByFolderName = new HashMap<String, Path>();

  private FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy;

  protected final Pattern folderSperatorPattern = Pattern.compile("[\\\\\\/]");
  // protected byte[] fileIndexExpectedMD5Sum; --For some reason, no md5sum is
  // required for fileIndex.xml in the standard
  protected byte[] tabelIndexExpectedMD5Sum;
  protected byte[] archiveIndexExpectedMD5Sum;
  protected boolean fileIndexIsParsed;

  public SIARDDK1007PathImportStrategy(SIARDArchiveContainer mainFolder, ReadStrategy readStrategy,
                                       MetadataPathStrategy metadataPathStrategy, String importAsSchema,
                                       FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy) {
    super();
    this.mainFolder = mainFolder;
    this.readStrategy = readStrategy;
    this.metadataPathStrategy = metadataPathStrategy;
    this.importAsSchema = importAsSchema;
    this.fileIndexXsdInputStreamStrategy = fileIndexXsdInputStreamStrategy;
  }

  public void parseFileIndexMetadata() throws ModuleException {

    if (!fileIndexIsParsed) {
      JAXBContext context;
      try {
        context = JAXBContext.newInstance(FileIndexType.class.getPackage().getName());
      } catch (JAXBException e) {
        throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
      }

      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema xsdSchema = null;
      InputStream xsdStream = fileIndexXsdInputStreamStrategy.getInputStream(this);
      try {
        xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
      } catch (SAXException e) {
        throw new ModuleException()
          .withMessage(
            "Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX))
          .withCause(e);
      }
      InputStream reader = null;
      FileIndexType xmlFileIndex;
      Unmarshaller unmarshaller;
      try {
        unmarshaller = context.createUnmarshaller();
        unmarshaller.setSchema(xsdSchema);
        reader = readStrategy.createInputStream(mainFolder,
          metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX));
        @SuppressWarnings("unchecked")
        JAXBElement<FileIndexType> jaxbElement = (JAXBElement<FileIndexType>) unmarshaller.unmarshal(reader);
        xmlFileIndex = jaxbElement.getValue();
      } catch (JAXBException e) {
        throw new ModuleException().withMessage("Error while Unmarshalling JAXB").withCause(e);
      } finally {
        try {
          xsdStream.close();
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          logger.debug("Could not close xsdStream", e);
        }
      }

      Pattern patternTableFolder = Pattern
        .compile("(AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.[0-9]*)\\\\Tables\\\\(table[0-9]*)");

      Pattern patternIndicesFolder = Pattern.compile("AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.1\\\\Indices");

      for (F fileInfo : xmlFileIndex.getF()) {
        Matcher mTblFldr = patternTableFolder.matcher(fileInfo.getFoN());
        if (mTblFldr.matches()) {
          String folderName = mTblFldr.group(2);
          Path archivePath = FileSystems.getDefault().getPath(mTblFldr.group(1));
          archiveFolderLookupByFolderName.put(folderName, archivePath);
          if (fileInfo.getFiN().toLowerCase().endsWith(SIARDDKConstants.XML_EXTENSION)) {
            if (xmlFilePathLookupByFolderName.containsKey(folderName)) {
              throw new ModuleException().withMessage("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
                + " for table files. Multiple entries for the xml file for folder [" + folderName + "].");
            }
            xmlFilePathLookupByFolderName.put(folderName, fileInfo);
          } else {
            if (fileInfo.getFiN().toLowerCase().endsWith(SIARDDKConstants.XSD_EXTENSION)) {
              if (xsdFilePathLookupByFolderName.containsKey(folderName)) {
                throw new ModuleException().withMessage("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
                  + " for table files. Multiple entries for the xsd file for folder [" + folderName + "].");
              }
              xsdFilePathLookupByFolderName.put(folderName, fileInfo);
            }
          }
        } else {
          Matcher mIndicesFldr = patternIndicesFolder.matcher(fileInfo.getFoN());
          if (mIndicesFldr.matches()) {
            // please notice, that this is a rudimentary implementation, only
            // considering the files relevant for the SIARDDK import module.
            if (fileInfo.getFiN().equals(SIARDDKConstants.TABLE_INDEX + "." + SIARDDKConstants.XML_EXTENSION)) {
              tabelIndexExpectedMD5Sum = fileInfo.getMd5();
            } else if (fileInfo.getFiN().equals(SIARDDKConstants.ARCHIVE_INDEX + "." +
              SIARDDKConstants.XML_EXTENSION)) {
              archiveIndexExpectedMD5Sum = fileInfo.getMd5();
            }
            /*
               * else { if (fileInfo.getFiN().equals(SIARDDKConstants.FILE_INDEX + "." +
               * SIARDDKConstants.XML_EXTENSION)) { fileIndexExpectedMD5Sum =
               * fileInfo.getMd5(); }
               */

          }
        }
      }
      fileIndexIsParsed = true;
    }
  }

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
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
    return getTableXMLFileInfo(schemaName, tableId).getMd5();
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

  protected F getTableXMLFileInfo(String schemaName, String tableId) throws ModuleException {
    canLookupTable(schemaName, tableId);
    String folderName = folderNameLookupByTableId.get(tableId);
    canLookupXMLFilePath(folderName);
    return xmlFilePathLookupByFolderName.get(folderName);
  }

  protected F getTableXSDFileInfo(String schemaName, String tableId) throws ModuleException {
    canLookupTable(schemaName, tableId);
    String folderName = folderNameLookupByTableId.get(tableId);
    canLookupXSDFilePath(folderName);
    return xsdFilePathLookupByFolderName.get(folderName);
  }

  protected String buildPathSansArchiveFolderName(F fileInfo) {
    Path pathFolderSperatorNeutral = FileSystems.getDefault().getPath("",
      folderSperatorPattern.split(fileInfo.getFoN()));
    pathFolderSperatorNeutral = pathFolderSperatorNeutral.subpath(1, pathFolderSperatorNeutral.getNameCount());
    Path pathFolderSperatorNeutralWithFile = pathFolderSperatorNeutral.resolve(fileInfo.getFiN());
    return pathFolderSperatorNeutralWithFile.toString();
  }

  @Override
  public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
    return buildPathSansArchiveFolderName(getTableXSDFileInfo(schemaName, tableId));
  }

  public byte[] getTableXSDFileMD5(String schemaName, String tableId) throws ModuleException {
    return getTableXSDFileInfo(schemaName, tableId).getMd5();
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

  public byte[] getTabelIndexExpectedMD5Sum() throws ModuleException {
    if (tabelIndexExpectedMD5Sum == null && fileIndexIsParsed) {
      throw new ModuleException()
        .withMessage("Parsing of " + SIARDDKConstants.FILE_INDEX + "." + SIARDDKConstants.XML_EXTENSION
          + " did not provide a md5sum for " + SIARDDKConstants.TABLE_INDEX + "." + SIARDDKConstants.XML_EXTENSION);
    }
    return tabelIndexExpectedMD5Sum;
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
}
