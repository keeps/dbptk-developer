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
public class MetadataCheckConstraintValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCheckConstraintValidator.class);
  private final String MODULE_NAME;
  private static final String M_512 = "5.12";
  private static final String M_512_1 = "M_5.12-1";
  private static final String M_512_1_1 = "M_5.12-1-1";
  private static final String A_M_512_1_1 = "A_M_5.12-1-1";
  private static final String M_512_1_2 = "M_5.12-1-2";
  private static final String A_M_512_1_3 = "A_M_5.12-1-3";
  private boolean additionalCheckError = false;
  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataCheckConstraintValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_512);

    getValidationReporter().moduleValidatorHeader(M_512, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:checkConstraints/ns:checkConstraint",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read check constraint from SIARD file";
      setError(M_512_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_512_1, "Database has no check constraint");
      observer.notifyValidationStep(MODULE_NAME, M_512_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_512_1, CHECK_CONSTRAINT_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:checkConstraints/ns:checkConstraint");

    if (validateCheckConstraintName(nodes)) {
      validationOk(MODULE_NAME, M_512_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_512_1_1, ValidationReporterStatus.ERROR);
    }

    if (!additionalCheckError) {
      validationOk(MODULE_NAME, A_M_512_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_512_1_1, ValidationReporterStatus.ERROR);
    }

    if (validateCheckConstraintCondition(nodes)) {
      validationOk(MODULE_NAME, M_512_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_512_1_1, ValidationReporterStatus.ERROR);
    }

    validateAttributeDescription(nodes);
    validationOk(MODULE_NAME, A_M_512_1_3);

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.12-1-1 The Check Constraint name is mandatory in SIARD 2.1 specification
   * 
   * A_M_5.12-1-1 The Check Constraint name should be unique
   */
  private boolean validateCheckConstraintName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element checkConstraint = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(checkConstraint, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(checkConstraint, Constants.SCHEMA),
        Constants.TABLE, table, Constants.CHECK_CONSTRAINT, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(checkConstraint, Constants.NAME);

      if (validateXMLField(M_512_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(table + name)) {
          setError(A_M_512_1_1, String.format("Check Constraint name %s must be unique per table(%s)", name, path));
          additionalCheckError = true;
          hasErrors = true;
        }
        continue;
      }
      setError(A_M_512_1_1, String.format("Aborted because check constraint name is mandatory (%s)", path));
      additionalCheckError = true;
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.12-1-2 The Check Constraint condition is mandatory in SIARD 2.1
   * specification
   */
  private boolean validateCheckConstraintCondition(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element checkConstraint = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(checkConstraint, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(checkConstraint, Constants.TABLE), Constants.CHECK_CONSTRAINT,
        XMLUtils.getChildTextContext(checkConstraint, Constants.NAME));
      String condition = XMLUtils.getChildTextContext(checkConstraint, Constants.CONDITION);

      if (!validateXMLField(M_512_1_2, condition, Constants.CONDITION, true, false, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * A_M_5.12-1-3 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   */
  private void validateAttributeDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element checkConstraint = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(checkConstraint, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(checkConstraint, Constants.TABLE), Constants.CHECK_CONSTRAINT,
        XMLUtils.getChildTextContext(checkConstraint, Constants.NAME));
      String description = XMLUtils.getChildTextContext(checkConstraint, Constants.DESCRIPTION);

      validateXMLField(A_M_512_1_3, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
