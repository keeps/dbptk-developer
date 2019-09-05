package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

  private Set<String> checkDuplicates = new HashSet<>();
  private List<Element> roleList = new ArrayList<>();
  private NodeList users;
  private NodeList roles;

  public MetadataRoleValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_518_1, M_518_1_1, A_M_518_1_1, M_518_1_2, A_M_518_1_2, A_M_518_1_3);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_518);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_518, MODULE_NAME);

    validateMandatoryXSDFields(M_518_1, ROLE_TYPE, "/ns:siardArchive/ns:roles/ns:role");

    if (!readXMLMetadataRoleLevel()) {
      reportValidations(M_518_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (roleList.isEmpty()) {
      getValidationReporter().skipValidation(M_518_1, "Database has no roles");
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataRoleLevel() {
    try {
      String pathToEntry = validatorPathStrategy.getMetadataXMLPath();
      String xpathExpressionUser = "/ns:siardArchive/ns:users/ns:user/ns:name";
      users = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionUser,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      String xpathExpressionRoles = "/ns:siardArchive/ns:roles/ns:role/ns:name";
      roles = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionRoles,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      String xpathExpression = "/ns:siardArchive/ns:roles/ns:role";
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpression,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element role = (Element) nodes.item(i);
        roleList.add(role);
        String path = buildPath(ROLE, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(role, Constants.NAME);
        validateRoleName(name, path);

        path = buildPath(ROLE, name);
        String admin = XMLUtils.getChildTextContext(role, ADMIN);
        validateRoleAdmin(admin, path);

        String description = XMLUtils.getChildTextContext(role, Constants.DESCRIPTION);
        validateRoleDescription(description, path);
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read roles from SIARD file";
      setError(M_518_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.18-1-1 the role name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.18-1-1 The role name in SIARD file should be unique
   */
  private void validateRoleName(String name, String path) {
    if(validateXMLField(M_518_1_1, name, Constants.NAME, true, false, path)){
      if (!checkDuplicates.add(name)) {
        addWarning(A_M_518_1_1, String.format("Role name %s should be unique", name), path);
      }
      return;
    }
    setError(A_M_518_1_1, String.format("Aborted because role name is mandatory (%s)", path));
  }

  /**
   * M_5.18-1-2 The role admin in SIARD file should exist. WARNING if it is not a
   * user or role
   */
  private void validateRoleAdmin(String admin, String path) {
    if(validateXMLField(M_518_1_2, admin, ADMIN, true, false, path)){
      for (int i = 0; i < users.getLength(); i++) {
        if (users.item(i).getTextContent().equals(admin)) {
          return;
        }
      }

      for (int i = 0; i < roles.getLength(); i++) {
        if (roles.item(i).getTextContent().equals(admin)) {
          return;
        }
      }
      addWarning(A_M_518_1_2, String.format("Admin %s should be an existing user or role", admin), path);
      return;
    }
    setError(A_M_518_1_2, String.format("Aborted because role admin is mandatory (%s)", path));
  }

  /**
   * A_M_5.18-1-3 The role description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateRoleDescription(String description, String path) {
    validateXMLField(A_M_518_1_3, description, Constants.DESCRIPTION, false, true, path);
  }
}
