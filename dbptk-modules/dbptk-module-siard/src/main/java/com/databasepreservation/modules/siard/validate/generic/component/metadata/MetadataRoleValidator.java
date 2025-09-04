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
public class MetadataRoleValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataRoleValidator.class);
  private final String MODULE_NAME;
  private static final String M_518 = "5.18";
  private static final String M_518_1 = "M_5.18-1";
  private static final String M_518_1_1 = "M_5.18-1-1";
  private static final String A_M_518_1_1 = "A_M_5.18-1-1";
  private static final String M_518_1_2 = "M_5.18-1-2";
  private static final String A_M_518_1_2 = "A_M_5.18-1-2";
  private static final String A_M_518_1_3 = "A_M_5.18-1-3";
  private static final String ADMIN = "admin";
  private static final String ROLE = "role";
  private boolean additionalCheckError = false;

  private Set<String> checkDuplicates = new HashSet<>();
  private NodeList users;
  private NodeList roles;

  public MetadataRoleValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    users = null;
    roles = null;
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_518);

    getValidationReporter().moduleValidatorHeader(M_518, MODULE_NAME);

    NodeList nodes;
    try (
      InputStream usersInputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath());
      InputStream rolesInputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath());
      InputStream nodesInputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath());) {
      users = (NodeList) XMLUtils.getXPathResult(usersInputStream,
        "/ns:siardArchive/ns:users/ns:user/ns:name", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      roles = (NodeList) XMLUtils.getXPathResult(rolesInputStream,
        "/ns:siardArchive/ns:roles/ns:role/ns:name", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      nodes = (NodeList) XMLUtils.getXPathResult(nodesInputStream,
        "/ns:siardArchive/ns:roles/ns:role", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read roles from SIARD file";
      setError(M_518_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_518_1, "Database has no roles");
      observer.notifyValidationStep(MODULE_NAME, M_518_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_518_1, ROLE_TYPE, "/ns:siardArchive/ns:roles/ns:role");

    if (validateRoleName(nodes)) {
      validationOk(MODULE_NAME, M_518_1_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_518_1_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_518_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_518_1_1);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_518_1_1, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_518_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateRoleAdmin(nodes)) {
      validationOk(MODULE_NAME, M_518_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_518_1_2, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_518_1_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_518_1_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_518_1_2);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_518_1_2, ValidationReporterStatus.ERROR);
      }
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_518_1_3, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_518_1_3, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateRoleDescription(nodes);
      validationOk(MODULE_NAME, A_M_518_1_3);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.18-1-1 the role name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.18-1-1 The role name in SIARD file should be unique
   */
  private boolean validateRoleName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element role = (Element) nodes.item(i);
      String path = buildPath(ROLE, Integer.toString(i));

      String name = XMLUtils.getChildTextContext(role, Constants.NAME);

      if (validateXMLField(M_518_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(name)) {
          addWarning(A_M_518_1_1, String.format("Role name %s should be unique", name), path);
        }
        continue;
      }
      setError(A_M_518_1_1, String.format("Aborted because role name is mandatory (%s)", path));
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.18-1-2 The role admin in SIARD file should exist. WARNING if it is not a
   * user or role
   */
  private boolean validateRoleAdmin(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element role = (Element) nodes.item(i);
      String path = buildPath(ROLE, XMLUtils.getChildTextContext(role, Constants.NAME));

      String admin = XMLUtils.getChildTextContext(role, ADMIN);

      if (validateXMLField(M_518_1_2, admin, ADMIN, true, false, path)) {
        boolean foundUserOrRole = false;
        for (int j = 0; j < users.getLength(); j++) {
          if (users.item(j).getTextContent().equals(admin)) {
            foundUserOrRole = true;
            break;
          }
        }

        for (int j = 0; j < roles.getLength(); j++) {
          if (roles.item(i).getTextContent().equals(admin)) {
            foundUserOrRole = true;
            break;
          }
        }
        if (!foundUserOrRole && !this.skipAdditionalChecks) {
          addWarning(A_M_518_1_2, String.format("Admin %s should be an existing user or role", admin), path);
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_518_1_2, String.format("Aborted because role admin is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * A_M_5.18-1-3 The role description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateRoleDescription(NodeList nodes) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element role = (Element) nodes.item(i);
      String path = buildPath(ROLE, XMLUtils.getChildTextContext(role, Constants.NAME));
      String description = XMLUtils.getChildTextContext(role, Constants.DESCRIPTION);

      validateXMLField(A_M_518_1_3, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
