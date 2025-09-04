/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.generic.component.metadata;

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
public class MetadataTableValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTableValidator.class);
  private final String MODULE_NAME;
  private static final String M_55 = "5.5";
  private static final String M_551 = "M_5.5-1";
  private static final String M_551_1 = "M_5.5-1-1";
  private static final String A_M_551_1 = "A_M_5.5-1-1";
  private static final String M_551_2 = "M_5.5-1-2";
  private static final String A_M_551_3 = "A_M_5.5-1-3";
  private static final String M_551_4 = "M_5.5-1-4";
  private static final String M_551_10 = "M_5.5-1-10";

  public MetadataTableValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_55);
    getValidationReporter().moduleValidatorHeader(M_55, MODULE_NAME);

    validateMandatoryXSDFields(M_551, TABLE_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table");

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read tables from SIARD file";
      setError(M_551, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (validateTableNames(nodes)) {
      validationOk(MODULE_NAME, M_551_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_551_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_551_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_551_1);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_551_1, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_551_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateTableFolders(nodes)) {
      validationOk(MODULE_NAME, M_551_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_551_2, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_551_3, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_551_3, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateTableDescriptions(nodes);
      validationOk(MODULE_NAME, A_M_551_3);
    }

    if (validateTableColumns(nodes)) {
      validationOk(MODULE_NAME, M_551_4);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_551_4, ValidationReporterStatus.ERROR);
    }

    if (validateTableRows(nodes)) {
      validationOk(MODULE_NAME, M_551_10);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_551_10, ValidationReporterStatus.ERROR);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.5-1-1 The table name is mandatory in SIARD 2.1 specification
   */
  private boolean validateTableNames(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element table = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(table, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(table, Constants.NAME);

      if (validateXMLField(M_551_1, name, Constants.NAME, true, false, path)) {
        if (!this.skipAdditionalChecks) {
          validateXMLField(A_M_551_1, name, Constants.NAME, false, true, path);
        }
        continue;
      }
      hasErrors = true;
      setError(A_M_551_1, String.format("Aborted because table name is mandatory (%s)", path));
    }

    return !hasErrors;
  }

  /**
   * M_5.5-1-2 The table folder is mandatory in SIARD 2.1 specification
   */
  private boolean validateTableFolders(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element table = (Element) nodes.item(i);
      // schema/table/name
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(table, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getChildTextContext(table, Constants.NAME));
      String folder = XMLUtils.getChildTextContext(table, Constants.FOLDER);

      if (!validateXMLField(M_551_2, folder, Constants.FOLDER, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.5-1-3 The table description in SIARD file must not be less than 3
   * characters.
   */
  private void validateTableDescriptions(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element table = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(table, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getChildTextContext(table, Constants.NAME));
      String description = XMLUtils.getChildTextContext(table, Constants.DESCRIPTION);
      validateXMLField(A_M_551_3, description, Constants.DESCRIPTION, false, true, path);
    }
  }

  /**
   * M_5.5-1-4 The table columns is mandatory in SIARD 2.1 specification
   */
  private boolean validateTableColumns(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element table = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(table, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getChildTextContext(table, Constants.NAME));
      String columns = XMLUtils.getChildTextContext(table, Constants.COLUMNS);

      if (!validateXMLField(M_551_4, columns, Constants.COLUMNS, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * M_5.5-1-10 The table rows is mandatory in SIARD 2.1 specification
   */
  private boolean validateTableRows(NodeList nodes) {
    boolean hasErrors = false;

    for (int i = 0; i < nodes.getLength(); i++) {
      Element table = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(table, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getChildTextContext(table, Constants.NAME));
      String rows = XMLUtils.getChildTextContext(table, Constants.ROWS);

      if (!validateXMLField(M_551_10, rows, Constants.ROWS, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }
}
