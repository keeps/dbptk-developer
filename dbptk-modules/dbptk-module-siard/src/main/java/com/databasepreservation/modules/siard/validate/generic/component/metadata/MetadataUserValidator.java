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
public class MetadataUserValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataUserValidator.class);
  private final String MODULE_NAME;
  private static final String M_517 = "5.17";
  private static final String M_517_1 = "M_5.17-1";
  private static final String M_517_1_1 = "M_5.17-1-1";
  private static final String A_M_517_1_1 = "A_M_5.17-1-1";
  private static final String A_M_517_1_2 = "A_M_5.17-1-2";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataUserValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_517);
    getValidationReporter().moduleValidatorHeader(M_517, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type/ns:attributes/ns:attribute", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read users from SIARD file";
      setError(M_517_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    validateMandatoryXSDFields(M_517_1, USER_TYPE, "/ns:siardArchive/ns:users/ns:user");

    if (validateUserName(nodes)) {
      validationOk(MODULE_NAME, M_517_1_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_517_1_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_517_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_517_1_1);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_517_1_1, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_517_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_517_1_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_517_1_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateUserDescription(nodes);
      validationOk(MODULE_NAME, A_M_517_1_2);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_517_1_1 is mandatory in siard 2.1 specification
   *
   * A_M_5.17-1-1 The user name in SIARD file should be unique
   */
  private boolean validateUserName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element user = (Element) nodes.item(i);
      String path = buildPath(Constants.USER, Integer.toString(i));

      String name = XMLUtils.getChildTextContext(user, Constants.NAME);

      if (validateXMLField(M_517_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(name)) {
          addWarning(A_M_517_1_1, String.format("User name %s should be unique", name), path);
        }
        continue;
      }
      setError(A_M_517_1_1, String.format("Aborted because user name is mandatory (%s)", path));
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * A_M_5.17-1-2 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   */
  private void validateUserDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element user = (Element) nodes.item(i);
      String path = buildPath(Constants.USER, XMLUtils.getChildTextContext(user, Constants.NAME));
      String description = XMLUtils.getChildTextContext(user, Constants.DESCRIPTION);

      validateXMLField(A_M_517_1_2, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
