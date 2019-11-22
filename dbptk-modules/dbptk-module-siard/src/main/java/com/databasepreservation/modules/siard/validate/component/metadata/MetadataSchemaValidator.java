/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataSchemaValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataSchemaValidator.class);
  private final String MODULE_NAME;
  private static final String M_52 = "5.2";
  private static final String M_521 = "M_5.2-1";
  private static final String M_521_1 = "M_5.2-1-1";
  private static final String A_M_521_1 = "A_M_5.2-1-1";
  private static final String M_521_2 = "M_5.2-1-2";
  private static final String A_M_521_2 = "A_M_5.2-1-2";
  private static final String A_M_521_4 = "A_M_5.2-1-4";

  public MetadataSchemaValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_52);
    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    validateMandatoryXSDFields(M_521, SCHEMA_TYPE, "/ns:siardArchive/ns:schemas/ns:schema");

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read schema from SIARD file";
      setError(M_521, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (validateSchemaNames(nodes)) {
      validationOk(MODULE_NAME, M_521_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_521_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_521_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_521_1);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_521_1, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_521_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateSchemaFolders(nodes)) {
      validationOk(MODULE_NAME, M_521_2);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_521_2, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_521_2, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_521_2);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_521_2, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_521_2, ValidationReporterStatus.ERROR);
      }
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_521_4, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_521_4, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateSchemaDescriptions(nodes);
      validationOk(MODULE_NAME, A_M_521_4);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.2-1-1 Schema Name is mandatory in SIARD 2.1 specification
   *
   * A_M_521_1 The schema name in the database must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   */
  private boolean validateSchemaNames(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(schema, Constants.NAME);

      if (validateXMLField(M_521_1, name, Constants.NAME, true, false, path)) {
        validateXMLField(A_M_521_1, name, Constants.NAME, false, true, path);
        continue;
      }
      hasErrors = true;
      setError(A_M_521_1, String.format("Aborted because schema name is mandatory (%s)", path));
    }

    return !hasErrors;
  }

  /**
   * M_5.2-1-2 Schema Folder is mandatory in SIARD 2.1 specification
   *
   * A_M_5.2-1-2 The schema folder in the database must not be empty. ERROR when
   * it is empty, WARNING if it is less than 3 characters
   *
   */
  private boolean validateSchemaFolders(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getChildTextContext(schema, Constants.NAME));
      String folder = XMLUtils.getChildTextContext(schema, Constants.FOLDER);

      if (validateXMLField(M_521_2, folder, Constants.FOLDER, true, false, path)) {
        validateXMLField(A_M_521_2, folder, Constants.FOLDER, false, true, path);
        continue;
      }
      hasErrors = true;
      setError(A_M_521_2, String.format("Aborted because schema folder is mandatory (%s)", path));
    }

    return !hasErrors;
  }

  /**
   * A_M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   */
  private void validateSchemaDescriptions(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element schema = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getChildTextContext(schema, Constants.NAME));
      String description = XMLUtils.getChildTextContext(schema, Constants.DESCRIPTION);
      validateXMLField(A_M_521_4, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
