package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.siard.bindings.siard_2_1.SiardArchive;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataXMLAgainstXSDValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataXMLAgainstXSDValidator.class);
  private static final String M_5 = "Requirements for metadata";
  private static final String M_501 = "M_5.0-1";

  private SIARDArchiveContainer container;
  private ReadStrategy readStrategy;
  private MetadataPathStrategy metadataPathStrategy;

  private static final String METADATA_FILENAME = "metadata";

  public static MetadataXMLAgainstXSDValidator newInstance() {
    return new MetadataXMLAgainstXSDValidator();
  }

  private MetadataXMLAgainstXSDValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader("5.0", M_5);
    try {
      getMetadata();
    } catch (ModuleException e) {
      getValidationReporter().validationStatus(M_501, ValidationReporter.Status.ERROR);
      return false;
    }
    getValidationReporter().validationStatus(M_501, ValidationReporter.Status.OK);

    return true;
  }

  private void getMetadata() throws ModuleException {

    metadataPathStrategy = new SIARD2MetadataPathStrategy();
    ContentPathImportStrategy contentPathStrategy = new SIARD2ContentPathImportStrategy();
    container = new SIARDArchiveContainer(getSIARDPackagePath(), SIARDArchiveContainer.OutputContainerType.MAIN);
    readStrategy = new ZipAndFolderReadStrategy(container);

    try {
      readStrategy.setup(container);
    } catch (ModuleException e) {
      LOGGER.debug("Problem setting up container", e);
    }

    MetadataImportStrategy metadataImportStrategy;
    switch (container.getVersion()) {
      case V2_0:
        metadataImportStrategy = new SIARD20MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_1:
        metadataImportStrategy = new SIARD21MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case DK:
      case V1_0:
      default:
        throw new ModuleException().withMessage("Metadata editing only supports SIARD 2 version");
    }

    metadataImportStrategy.setOnceReporter(getReporter());

    try {
      validateMetadata();
    } catch (NullPointerException e) {
      throw new ModuleException().withMessage("Metadata editing only supports SIARD 2 version").withCause(e);
    } finally {
      readStrategy.finish(container);
    }
  }

  private void validateMetadata() throws ModuleException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SiardArchive.class.getPackage().getName(), SiardArchive.class.getClassLoader());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema;
    InputStream xsdStream = readStrategy.createInputStream(container,
      metadataPathStrategy.getXsdFilePath(METADATA_FILENAME));
    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdStream));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + metadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    }

    InputStream reader = null;
    Unmarshaller unmarshaller;

    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);
      reader = readStrategy.createInputStream(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
      unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      LOGGER.error("The metadata.xml file did not pass the XML Schema validation.", e);
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
          LOGGER.trace("problem closing reader after XMl validation failure", e1);
        }
        throw new ModuleException().withMessage("Error while Unmarshalling JAXB with XSD").withCause(e);
      }
      try {
        unmarshaller = context.createUnmarshaller();
        reader = readStrategy.createInputStream(container, metadataPathStrategy.getXmlFilePath(METADATA_FILENAME));
        unmarshaller.unmarshal(reader);
      } catch (JAXBException e1) {
        throw new ModuleException().withMessage("The metadata.xml file could not be read.").withCause(e1);
      }
    } finally {
      try {
        xsdStream.close();
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.debug("Could not close xsdStream", e);
      }
    }
  }
}
