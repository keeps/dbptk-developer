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
public class MetadataTypeValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTypeValidator.class);
  private final String MODULE_NAME;
  private static final String M_53 = "5.3";
  private static final String M_531 = "M_5.3-1";
  private static final String M_531_1 = "M_5.3-1-1";
  private static final String M_531_2 = "M_5.3-1-2";
  private static final String M_531_5 = "M_5.3-1-5";
  private static final String M_531_6 = "M_5.3-1-6";
  private static final String A_M_531_10 = "A_M_5.3-1-10";

  public MetadataTypeValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_53);

    getValidationReporter().moduleValidatorHeader(M_53, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read types from SIARD file";
      setError(M_531, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_531, "Database has no types");
      observer.notifyValidationStep(MODULE_NAME, M_531, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_531, TYPE_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type");

    if (validateTypeName(nodes)) {
      validationOk(MODULE_NAME, M_531_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_531_1, ValidationReporterStatus.ERROR);
    }

    if (validateTypeCategory(nodes)) {
      validationOk(MODULE_NAME, M_531_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_531_2, ValidationReporterStatus.ERROR);
    }

    if (validateTypeInstantiable(nodes)) {
      validationOk(MODULE_NAME, M_531_5);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_531_5, ValidationReporterStatus.ERROR);
    }

    if (validateTypeFinal(nodes)) {
      validationOk(MODULE_NAME, M_531_6);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_531_6, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_531_10, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_531_10, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateTypeDescription(nodes);
      validationOk(MODULE_NAME, A_M_531_10);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.3-1-1 The type name is mandatory in SIARD 2.1 specification
   */
  private boolean validateTypeName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element type = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(type, Constants.SCHEMA), Constants.TYPE,
        Integer.toString(i));
      String name = XMLUtils.getChildTextContext(type, Constants.NAME);

      if (!validateXMLField(M_531_1, name, Constants.NAME, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * M_5.3-1-2 The type category is mandatory in SIARD 2.1 specification
   *
   * must be distinct or udt
   */
  private boolean validateTypeCategory(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element type = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(type, Constants.SCHEMA), Constants.TYPE,
        XMLUtils.getChildTextContext(type, Constants.NAME));
      String category = XMLUtils.getChildTextContext(type, Constants.CATEGORY);

      if (category == null || category.isEmpty()) {
        setError(M_531_2, buildErrorMessage(Constants.CATEGORY, path));
        hasErrors = true;
      } else if (!category.equals(Constants.DISTINCT) && !category.equals(Constants.UDT)) {
        setError(M_531_2, String.format("type category must be 'distinct' or 'udt' (%s)", path));
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * M_5.3-1-5 The type instantiable is mandatory in SIARD 2.1 specification
   *
   * must be true or false
   */
  private boolean validateTypeInstantiable(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element type = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(type, Constants.SCHEMA), Constants.TYPE,
        XMLUtils.getChildTextContext(type, Constants.NAME));
      String instantiable = XMLUtils.getChildTextContext(type, Constants.TYPE_INSTANTIABLE);

      if (instantiable == null || instantiable.isEmpty()) {
        setError(M_531_5, buildErrorMessage(Constants.TYPE_INSTANTIABLE, path));
        hasErrors = true;
      } else if (!instantiable.equals(Constants.TRUE) && !instantiable.equals(Constants.FALSE)) {
        setError(M_531_5, String.format("type instantiable must be 'true' or 'false' (%s)", path));
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * M_5.3-1-6 The type final field is mandatory in SIARD 2.1 specification
   *
   * must be true or false
   */
  private boolean validateTypeFinal(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element type = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(type, Constants.SCHEMA), Constants.TYPE,
        XMLUtils.getChildTextContext(type, Constants.NAME));
      String typeFinal = XMLUtils.getChildTextContext(type, Constants.TYPE_FINAL);

      if (typeFinal == null || typeFinal.isEmpty()) {
        setError(M_531_6, buildErrorMessage(Constants.TYPE_FINAL, path));
        hasErrors = true;
      } else if (!typeFinal.equals(Constants.TRUE) && !typeFinal.equals(Constants.FALSE)) {
        setError(M_531_6, String.format("type final must be 'true' or 'false' (%s)", path));
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.3-1-10 The type description field in the schema must not be must not be
   * less than 3 characters. WARNING if it is less than 3 characters
   */
  private void validateTypeDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element type = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(type, Constants.SCHEMA), Constants.TYPE,
        XMLUtils.getChildTextContext(type, Constants.NAME));
      String description = XMLUtils.getChildTextContext(type, Constants.DESCRIPTION);

      validateXMLField(A_M_531_10, description, Constants.DESCRIPTION, false, true, path);
    }
  }

}
