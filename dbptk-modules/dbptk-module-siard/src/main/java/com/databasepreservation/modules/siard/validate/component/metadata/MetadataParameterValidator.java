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
public class MetadataParameterValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataParameterValidator.class);
  private final String MODULE_NAME;
  private static final String M_516 = "5.16";
  private static final String M_516_1 = "M_5.16-1";
  private static final String M_516_1_1 = "M_5.16-1-1";
  private static final String A_M_516_1_1 = "A_M_5.16-1-1";
  private static final String M_516_1_2 = "M_5.16-1-2";
  private static final String A_M_516_1_8 = "A_M_5.16-1-8";
  private boolean additionalCheckError = false;

  private static final String IN = "IN";
  private static final String OUT = "OUT";
  private static final String INOUT = "INOUT";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataParameterValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_516);
    getValidationReporter().moduleValidatorHeader(M_516, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine/ns:parameters/ns:parameter",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read parameters from SIARD file";
      setError(M_516_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_516_1, "Database has no parameters");
      observer.notifyValidationStep(MODULE_NAME, M_516_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_516_1, PARAMETER_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine/ns:parameters/ns:parameter");

    if (validateParameterName(nodes)) {
      validationOk(MODULE_NAME, M_516_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_516_1_1, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_516_1_1, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_516_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_516_1_1);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_516_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateParameterMode(nodes)) {
      validationOk(MODULE_NAME, M_516_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_516_1_2, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_516_1_8, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_516_1_8, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateParameterDescription(nodes);
      validationOk(MODULE_NAME, A_M_516_1_8);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_516_1_1 The parameter name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.16-1-1 The parameter name in SIARD file should be unique. WARNING when
   * it is empty or not unique
   *
   */
  private boolean validateParameterName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element parameter = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(parameter, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.PARAMETER, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(parameter, Constants.NAME);

      if (validateXMLField(M_516_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(name) && !this.skipAdditionalChecks) {
          addWarning(A_M_516_1_1, String.format("Parameter name %s should be unique", name), path);
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_516_1_1, String.format("Aborted because parameter name is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.16-1-2 The parameter mode in SIARD file should be IN, OUT or INOUT and
   * mandatory. WARNING when it is empty
   *
   */
  private boolean validateParameterMode(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element parameter = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(parameter, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.PARAMETER,
        XMLUtils.getChildTextContext(parameter, Constants.NAME));
      String mode = XMLUtils.getChildTextContext(parameter, Constants.PARAMETER_MODE);

      if (validateXMLField(M_516_1_2, mode, Constants.PARAMETER_MODE, true, false, path)) {
        switch (mode) {
          case IN:
          case OUT:
          case INOUT:
            break;
          default:
            setError(M_516_1_2, String.format("Mode '%s' is not allowed (%s)", mode, path));
            hasErrors = true;
        }
      } else {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.16-1-8 The parameter description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateParameterDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element parameter = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(parameter, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.PARAMETER,
        XMLUtils.getChildTextContext(parameter, Constants.NAME));
      String description = XMLUtils.getChildTextContext(parameter, Constants.DESCRIPTION);

      validateXMLField(A_M_516_1_8, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
