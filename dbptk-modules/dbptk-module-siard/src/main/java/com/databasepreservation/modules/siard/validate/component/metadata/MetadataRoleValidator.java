package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataRoleValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Role level metadata";
  private static final String M_519 = "5.19";
  private static final String M_519_1 = "M_5.19-1";
  private static final String M_519_1_1 = "M_5.19-1-1";
  private static final String M_519_1_2 = "M_5.19-1-2";
  private static final String M_519_1_3 = "M_5.19-1-3";
  private static final String ADMIN = "admin";
  private static final String ROLE = "role";

  private Set<String> checkDuplicates = new HashSet<>();
  private NodeList users;
  private NodeList roles;

  public static MetadataRoleValidator newInstance() {
    return new MetadataRoleValidator();
  }

  private MetadataRoleValidator() {

  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;
    getValidationReporter().moduleValidatorHeader(M_519, MODULE_NAME);

    if (!readXMLMetadataRoleLevel()) {
      reportValidations(M_519_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_519_1, MODULE_NAME) && reportValidations(M_519_1_1, MODULE_NAME)
      && reportValidations(M_519_1_2, MODULE_NAME) && reportValidations(M_519_1_3, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
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

        String name = MetadataXMLUtils.getChildTextContext(role, Constants.NAME);
        String path = buildPath(ROLE, name);
        if (!validateRoleName(name, path))
          break;

        String admin = MetadataXMLUtils.getChildTextContext(role, ADMIN);
        if (!validateRoleAdmin(admin, path))
          break;

        String description = MetadataXMLUtils.getChildTextContext(role, Constants.DESCRIPTION);
        if (!validateRoleDescription(description, path))
          break;
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.19-1-1 The role name in SIARD file should be unique. ERROR when it is
   * empty, WARNING when it is not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoleName(String name, String path) {
    if (!validateXMLField(M_519_1, name, Constants.NAME, true, false, path)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      addWarning(M_519_1_1, String.format("Role name %s should be unique", name), path);
    }
    return true;
  }

  /**
   * M_5.19-1-1 The role admin in SIARD file should exist. WARNING if it is not a
   * user or role
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoleAdmin(String admin, String path) {
    if (admin == null || admin.isEmpty()) {
      setError(M_519_1_2, String.format("Role admin inside %s is mandatory", path));
      return false;
    }

    for (int i = 0; i < users.getLength(); i++) {
      if (users.item(i).getTextContent().equals(admin)) {
        return true;
      }
    }

    for (int i = 0; i < roles.getLength(); i++) {
      if (roles.item(i).getTextContent().equals(admin)) {
        return true;
      }
    }

    addWarning(M_519_1_2, String.format("Admin %s should be an existing user or role", admin), path);
    return true;
  }

  /**
   * M_5.19-1-2 The role description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoleDescription(String description, String path) {
    return validateXMLField(M_519_1_3, description, Constants.DESCRIPTION, false, true, path);
  }
}
