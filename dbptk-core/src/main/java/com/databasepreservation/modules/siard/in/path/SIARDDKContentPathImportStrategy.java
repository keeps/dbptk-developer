package com.databasepreservation.modules.siard.in.path;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import dk.sa.xmlns.diark._1_0.fileindex.FileIndexType;
import dk.sa.xmlns.diark._1_0.fileindex.FileIndexType.F;
/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKContentPathImportStrategy implements ContentPathImportStrategy {
  protected final CustomLogger logger = CustomLogger.getLogger(ContentPathImportStrategy.class);
  protected final String importAsSchema;
  protected final SIARDArchiveContainer mainFolder;
  protected final ReadStrategy readStrategy;
  protected final MetadataPathStrategy metadataPathStrategy;
  protected final Map<String, F> xmlFilePathLookupByFolderName = new HashMap<String, F>();
  protected final Map<String, F> xsdFilePathLookupByFolderName = new HashMap<String, F>();
  protected final Map<String, String> folderNameLookupByTableId = new HashMap<String, String>();

  public SIARDDKContentPathImportStrategy(SIARDArchiveContainer mainFolder, ReadStrategy readStrategy,
    MetadataPathStrategy metadataPathStrategy, String importAsSchema) {
    super();
    this.mainFolder = mainFolder;
    this.readStrategy = readStrategy;
    this.metadataPathStrategy = metadataPathStrategy;
    this.importAsSchema = importAsSchema;
  }

  public void init() throws ModuleException {

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(FileIndexType.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException("Error loading JAXBContext", e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdStream = readStrategy.createInputStream(mainFolder,
      metadataPathStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
    } catch (SAXException e) {
      throw new ModuleException(
        "Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX), e);
    }
    InputStream reader = null;
    FileIndexType xmlFileIndex;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      // unmarshaller.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      // unmarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
      unmarshaller.setSchema(xsdSchema);
      // TODO: Validate file md5sum
      reader = readStrategy.createInputStream(mainFolder,
        metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX));
      xmlFileIndex = (FileIndexType) unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      throw new ModuleException("Error while Unmarshalling JAXB", e);
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

    Pattern pattrnTableFolder = Pattern.compile("AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.[0-9]*\\Tables\\(table[0-9]*)");

    for (F fileInfo : xmlFileIndex.getF()) {
      Matcher m = pattrnTableFolder.matcher(fileInfo.getFoN());
      if (m.matches()) {
        String folderName=m.group(1);
        if( fileInfo.getFiN().toLowerCase().endsWith(SIARDDKConstants.XML_EXTENSION))
        {
          if(xmlFilePathLookupByFolderName.containsKey(folderName))
          {
            throw new ModuleException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
              + " for table files. Multiple entries for the xml file for folder [" + folderName + "].");
          }
          xmlFilePathLookupByFolderName.put(folderName, fileInfo);
        }
 else {
          if (fileInfo.getFiN().toLowerCase().endsWith(SIARDDKConstants.XSD_EXTENSION)) {
            if (xsdFilePathLookupByFolderName.containsKey(folderName)) {
              throw new ModuleException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
                + " for table files. Multiple entries for the xsd file for folder [" + folderName + "].");
            }
            xsdFilePathLookupByFolderName.put(folderName, fileInfo);
          }
        }
        // TODO: choke on unknown file extensions
      }
    }

    /*
     * /* /* <foN>AVID.SA.18000.1\Tables\table2</foN> <fiN>table2.xsd</fiN>
     * <md5>1193BCAF1511C34DF0F2DAFFC63E78F7</md5>
     * 
     */
  }

  @Override
  public String getLobPath(String basePath, String schemaName, String tableId, String columnId, String lobFileName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void associateSchemaWithFolder(String schemaName, String schemaFolder) {
    // Not relevant for SIARDDK
  }

  @Override
  public void associateTableWithFolder(String tableId, String tableFolder) {
    folderNameLookupByTableId.put(tableId, tableFolder);
  }

  @Override
  public void associateColumnWithFolder(String columnId, String columnFolder) {
    // TODO Auto-generated method stub

  }

  protected void canLookupTable(String schemaName, String tableId) throws ModuleException {
    if (!schemaName.equals(schemaName)) {
      throw new ModuleException("SIARDDK does not support multiple schemas. The given schema [" + schemaName
        + "] is not identical to the schema name given on start up: [" + this.importAsSchema + "]");
    }

    if (!folderNameLookupByTableId.containsKey(tableId)) {
      throw new ModuleException(
        "No folder name has - during the parsing of the database sctructure - been associated with the given table id:"
          + tableId);
    }
  }

  protected void canLookupXMLFilePath(String folderName) throws ModuleException {
    if (!xmlFilePathLookupByFolderName.containsKey(folderName)) {
      throw new ModuleException(
        "No xml file path has - during the parsing of the file index - been associated with the folder name:"
          + folderName);
    }
  }

  @Override
  public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
    return getTableXMLFileInfo(schemaName, tableId).getFiN();
  }

  public byte[] getTableXMLFileMD5(String schemaName, String tableId) throws ModuleException {
    return getTableXMLFileInfo(schemaName, tableId).getMd5();
  }

  protected void canLookupXSDFilePath(String folderName) throws ModuleException {
    if (!xsdFilePathLookupByFolderName.containsKey(folderName)) {
      throw new ModuleException(
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

  @Override
  public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
    return getTableXSDFileInfo(schemaName, tableId).getFiN();
  }

  public byte[] getTableXSDFileMD5(String schemaName, String tableId) throws ModuleException {
    return getTableXSDFileInfo(schemaName, tableId).getMd5();
  }

}
