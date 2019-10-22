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
import java.util.HashSet;
import java.util.Set;

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
public class MetadataViewValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataViewValidator.class);
  private final String MODULE_NAME;
  private static final String M_514 = "5.14";
  private static final String M_514_1 = "M_5.14-1";
  private static final String M_514_1_1 = "M_5.14-1-1";
  private static final String M_514_1_2 = "M_5.14-1-2";
  private static final String A_M_514_1_1 = "A_M_5.14-1-1";
  private static final String A_M_514_1_2 = "A_M_5.14-1-1";
  private static final String A_M_514_1_5 = "A_M_5.14-1-5";
  private boolean additionalCheckError = false;

  private static final int MIN_COLUMN_COUNT = 1;

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataViewValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_514);
    getValidationReporter().moduleValidatorHeader(M_514, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read views from SIARD file";
      setError(M_514_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_514_1, "Database has no view");
      observer.notifyValidationStep(MODULE_NAME, M_514_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_514_1, VIEW_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view");

    if (validateViewName(nodes)) {
      validationOk(MODULE_NAME, M_514_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_514_1_1, ValidationReporterStatus.ERROR);
    }

    if (!additionalCheckError) {
      validationOk(MODULE_NAME, A_M_514_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_514_1_1, ValidationReporterStatus.ERROR);
    }

    if (validateViewColumn(nodes)) {
      validationOk(MODULE_NAME, A_M_514_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_514_1_2, ValidationReporterStatus.ERROR);
    }

    validateAttributeDescription(nodes);
    validationOk(MODULE_NAME, A_M_514_1_5);

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.14-1-1 The view name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.14-1-1 The view name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   */
  private boolean validateViewName(NodeList nodes) {
    additionalCheckError = false;
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element view = (Element) nodes.item(i);
      String schema = XMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), Constants.NAME);
      String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(view, Constants.NAME);

      if (validateXMLField(M_514_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(name)) {
          setError(A_M_514_1_1, String.format("View name %s must be unique (%s)", name, path));
          hasErrors = true;
          additionalCheckError = true;
        }
        continue;
      }
      setError(A_M_514_1_1, String.format("Aborted because view name is mandatory (%s)", path));
      additionalCheckError = true;
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.14-1-2 The view list of columns is mandatory in siard, and must exist on
   * table column.
   *
   * A_M_5.14-1-2 should exist at least one column.
   */
  private boolean validateViewColumn(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element view = (Element) nodes.item(i);
      String schema = XMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), Constants.NAME);
      String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW,
        XMLUtils.getChildTextContext(view, Constants.NAME));
      NodeList columnsList = view.getElementsByTagName(Constants.COLUMN);

      if (columnsList.getLength() < MIN_COLUMN_COUNT) {
        setError(A_M_514_1_2, String.format("View must have at least '%d' column (%s)", MIN_COLUMN_COUNT, path));
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.14-1-5 The view description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateAttributeDescription(NodeList nodes) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element view = (Element) nodes.item(i);
      String schema = XMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), Constants.NAME);
      String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW,
        XMLUtils.getChildTextContext(view, Constants.NAME));
      String description = XMLUtils.getChildTextContext(view, Constants.DESCRIPTION);

      validateXMLField(A_M_514_1_5, description, Constants.DESCRIPTION, false, true, path);
    }
  }

}
