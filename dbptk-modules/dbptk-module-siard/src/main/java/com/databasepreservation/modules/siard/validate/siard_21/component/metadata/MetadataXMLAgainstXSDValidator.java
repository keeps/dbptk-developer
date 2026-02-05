/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.siard_21.component.metadata;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import com.databasepreservation.modules.siard.validate.common.SiardValidationErrorFormatter;
import com.databasepreservation.modules.siard.validate.common.SiardValidationErrorHandler;
import com.databasepreservation.modules.siard.validate.generic.component.metadata.MetadataValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.bindings.siard_2_1.SiardArchive;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataXMLAgainstXSDValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataXMLAgainstXSDValidator.class);
  private final String MODULE_NAME;
  private static final String M_50 = "5.0";
  private static final String M_501 = "M_5.0-1";

  public MetadataXMLAgainstXSDValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_50);

    getValidationReporter().moduleValidatorHeader(M_50, MODULE_NAME);
    if (validateXMLAgainstXSD()) {
      validationOk(MODULE_NAME, M_501);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_501, ValidationReporterStatus.ERROR);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.0-1 The schema definition metadata.xsd must be complied with for the
   * metadata.xml file. This means that metadata.xml must be capable of being
   * positively validated against metadata.xsd.
   */
  private boolean validateXMLAgainstXSD() {
    SiardValidationErrorHandler errorHandler = new SiardValidationErrorHandler();
    
    try (
      InputStream XSDInputStream = SiardArchive.class.getClassLoader()
        .getResourceAsStream("schema/siard2-1-metadata.xsd");
      InputStream XMLInputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath())) {

      Source schemaFile = new StreamSource(XSDInputStream);
      Source xmlFile = new StreamSource(XMLInputStream);

      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      Schema schema = schemaFactory.newSchema(schemaFile);
      Validator validator = schema.newValidator();
      
      // Set custom error handler to capture detailed validation errors
      validator.setErrorHandler(errorHandler);
      
      validator.validate(xmlFile);
    } catch (SAXException | IOException e) {
      // Process errors with enhanced formatting
      if (errorHandler.hasErrors()) {
        reportDetailedErrors(errorHandler);
      } else {
        // Fallback to basic error message if no detailed errors captured
        setError(M_501, e.getMessage());
      }
      return false;
    }

    // Check if any errors were captured even without exceptions
    if (errorHandler.hasErrors()) {
      reportDetailedErrors(errorHandler);
      return false;
    }

    return true;
  }

  /**
   * Reports detailed validation errors with context information.
   * 
   * @param errorHandler The error handler containing captured errors
   */
  private void reportDetailedErrors(SiardValidationErrorHandler errorHandler) {
    // Try to extract XML context for the first error
    SiardValidationErrorFormatter.SiardXmlContext xmlContext = null;
    
    if (!errorHandler.getAllErrors().isEmpty()) {
      SiardValidationErrorHandler.ValidationError firstError = errorHandler.getAllErrors().get(0);
      
      // Attempt to extract context from XML
      try (InputStream contextInputStream = zipFileManagerStrategy.getZipInputStream(path,
          validatorPathStrategy.getMetadataXMLPath())) {
        xmlContext = SiardValidationErrorFormatter.extractXmlContext(
          contextInputStream, 
          firstError.getLineNumber()
        );
      } catch (IOException e) {
        // Context extraction failed, continue without it
        LOGGER.debug("Failed to extract XML context for error at line {}", firstError.getLineNumber(), e);
      }
    }
    
    // Report all errors with formatted messages
    StringBuilder errorMessage = new StringBuilder();
    for (SiardValidationErrorHandler.ValidationError error : errorHandler.getAllErrors()) {
      if (errorMessage.length() > 0) {
        errorMessage.append("; ");
      }
      errorMessage.append(SiardValidationErrorFormatter.formatErrorMessage(error, xmlContext));
    }
    
    setError(M_501, errorMessage.toString());
  }
}