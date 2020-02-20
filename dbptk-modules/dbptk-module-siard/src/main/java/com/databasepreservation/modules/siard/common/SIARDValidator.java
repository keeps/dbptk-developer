/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.bindings.siard_2_1.SiardArchive;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD1MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD1ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWithExternalLobsWriteStrategy;
import com.databasepreservation.utils.ModuleConfigurationUtils;

public class SIARDValidator {
  private final SIARDArchiveContainer container;
  private final ReadStrategy readStrategy;
  private final MetadataPathStrategy metadataPathStrategy;
  private final ContentPathImportStrategy contentPathStrategy;
  private final MetadataImportStrategy metadataImportStrategy;
  private Reporter reporter;

  private static final String METADATA_FILENAME = "metadata";

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidator.class);

  public SIARDValidator(SIARDArchiveContainer container, WriteStrategy writeStrategy) {
    this.container = container;
    if (writeStrategy instanceof ZipWithExternalLobsWriteStrategy) {
      readStrategy = new ZipAndFolderReadStrategy(container);
    } else if (writeStrategy instanceof FolderWriteStrategy) {
      readStrategy = new FolderReadStrategy(container);
    } else {
      readStrategy = new ZipReadStrategy();
    }

    switch (container.getVersion()) {
      case V1_0:
        metadataPathStrategy = new SIARD1MetadataPathStrategy();
        contentPathStrategy = new SIARD1ContentPathImportStrategy();
        metadataImportStrategy = new SIARD1MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      case V2_0:
        metadataPathStrategy = new SIARD2MetadataPathStrategy();
        contentPathStrategy = new SIARD2ContentPathImportStrategy();
        metadataImportStrategy = new SIARD20MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
      default:
        metadataPathStrategy = new SIARD2MetadataPathStrategy();
        contentPathStrategy = new SIARD2ContentPathImportStrategy();
        metadataImportStrategy = new SIARD21MetadataImportStrategy(metadataPathStrategy, contentPathStrategy);
        break;
    }
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;

    metadataImportStrategy.setOnceReporter(reporter);
  }

  public void validateSIARD() throws ModuleException {
    LOGGER.info("Validating SIARD");

    readStrategy.setup(container);

    LOGGER.info("Validating metadata");
    validateMetadata();
    LOGGER.info("Metadata validated");

    LOGGER.info("Validating content");

    ModuleConfiguration moduleConfiguration = ModuleConfigurationUtils.getDefaultModuleConfiguration();
    metadataImportStrategy.loadMetadata(readStrategy, container, moduleConfiguration);
    DatabaseStructure dbStructure = metadataImportStrategy.getDatabaseStructure();

    validateContent(dbStructure);
    LOGGER.info("Content validated");

    readStrategy.finish(container);

    LOGGER.info("SIARD validated");
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
      LOGGER.warn("The metadata.xml file did not pass the XML Schema validation.",
        new ModuleException().withMessage("Error while Unmarshalling JAXB with XSD").withCause(e));
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
          LOGGER.trace("problem closing reader after XMl validation failure", e1);
        }
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

  private void validateContent(DatabaseStructure databaseStructure) {
    for (SchemaStructure schema : databaseStructure.getSchemas()) {
      LOGGER.info("Validating schema {}", schema.getName());
      for (TableStructure table : schema.getTables()) {
        LOGGER.info("Validating table {}", table.getName());
        try {
          // setup a new validating parser
          String tableXSDFileName = contentPathStrategy.getTableXSDFilePath(schema.getName(), table.getId());
          try (InputStream currentTableXSDStream = readStrategy.createInputStream(container, tableXSDFileName)) {
            // import values from XML
            String tableXMLFilename = contentPathStrategy.getTableXMLFilePath(schema.getName(), table.getId());
            try (InputStream currentTableXMLStream = readStrategy.createInputStream(container, tableXMLFilename)) {

              try {
                validateXMLAgainstXSD(currentTableXMLStream, currentTableXSDStream);
              } catch (SAXException | IOException e) {
                LOGGER.warn("XML file for table {} didn't pass validation", table.getId());
              }
            }
          } catch (IOException e) {
            LOGGER.error("An error occurred validating table {} contents", table.getId(), e);
          }

        } catch (ModuleException e) {
          LOGGER.error("An error occurred validating table {} contents", table.getId(), e);
        }
        LOGGER.info("Completed table {}", table.getName());
      }
      LOGGER.info("Completed schema {}", schema.getName());
    }
  }

  private void validateXMLAgainstXSD(InputStream xml, InputStream xsd) throws SAXException, IOException {
    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = factory.newSchema(new StreamSource(xsd));
    Validator validator = schema.newValidator();
    validator.validate(new StreamSource(xml));
  }
}
