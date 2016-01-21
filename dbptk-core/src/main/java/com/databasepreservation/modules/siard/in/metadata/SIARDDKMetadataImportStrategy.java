
package com.databasepreservation.modules.siard.in.metadata;

import java.io.IOException;
import java.io.InputStream;

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
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

import dk.sa.xmlns.diark._1_0.tableindex.SiardDiark;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKMetadataImportStrategy implements MetadataImportStrategy {

  protected static String METADATA_FILENAME = "tabelIndex";
  protected final CustomLogger logger = CustomLogger.getLogger(SIARDDKMetadataImportStrategy.class);

  protected final MetadataPathStrategy metadataPathStrategy;
  protected final ContentPathImportStrategy contentPathStrategy;
  protected DatabaseStructure databaseStructure;

  public SIARDDKMetadataImportStrategy(MetadataPathStrategy metadataPathStrategy,
    ContentPathImportStrategy contentPathImportStrategy) {
    this.metadataPathStrategy = metadataPathStrategy;
    this.contentPathStrategy = contentPathImportStrategy;
  }

  @Override
  public void loadMetadata(ReadStrategy readStrategy, SIARDArchiveContainer container) throws ModuleException {

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardDiark.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException("Error loading JAXBContext", e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdStream = readStrategy.createInputStream(container,
      metadataPathStrategy.getXsdFilePath(METADATA_FILENAME));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
    } catch (SAXException e) {
      throw new ModuleException(
        "Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(METADATA_FILENAME), e);
    }
    InputStream reader = null;
    SiardDiark xmlRoot;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      // unmarshaller.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      // unmarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION,
      unmarshaller.setSchema(xsdSchema);
      // TODO: Validate file md5sum
      reader = readStrategy.createInputStream(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
      xmlRoot = (SiardDiark) unmarshaller.unmarshal(reader);
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

    databaseStructure = getDatabaseStructure(xmlRoot);

  }

  @Override
  public DatabaseStructure getDatabaseStructure() throws ModuleException {
    if (databaseStructure != null) {
      return databaseStructure;
    } else {
      throw new ModuleException("getDatabaseStructure must not be called before loadMetadata");
    }
  }

  protected DatabaseStructure getDatabaseStructure(SiardDiark siardArchive) throws ModuleException {
    DatabaseStructure databaseStructure = new DatabaseStructure();
    // TODO:
    /*
     * databaseStructure.setDescription(siardArchive.getDescription());
     * databaseStructure.setArchiver(siardArchive.getArchiver());
     * databaseStructure.setArchiverContact(siardArchive.getArchiverContact());
     * databaseStructure.setDataOwner(siardArchive.getDataOwner());
     * databaseStructure.setDataOriginTimespan(siardArchive.
     * getDataOriginTimespan());
     * databaseStructure.setProducerApplication(siardArchive.
     * getProducerApplication());
     * databaseStructure.setArchivalDate(JodaUtils.xs_date_parse(siardArchive.
     * getArchivalDate()));
     * databaseStructure.setClientMachine(siardArchive.getClientMachine());
     * databaseStructure.setDatabaseUser(siardArchive.getDatabaseUser());
     */
    databaseStructure.setName(siardArchive.getDbName());
    databaseStructure.setProductName(siardArchive.getDatabaseProduct());
    // TODO: Continue here (set schema according to input param)

    return databaseStructure;

  }

}
