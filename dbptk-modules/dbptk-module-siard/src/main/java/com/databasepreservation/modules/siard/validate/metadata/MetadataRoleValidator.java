package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;

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

  private Set<String> checkDuplicates = new HashSet<>();
  private NodeList users;
  private NodeList roles;

  public static MetadataRoleValidator newInstance() {
    return new MetadataRoleValidator();
  }

  private MetadataRoleValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_519, MODULE_NAME);
    if (!readXMLMetadataUserLevel()) {
      setError(M_519_1, "Cannot read roles");
    }

    return reportValidations(M_519_1) && reportValidations(M_519_1_1) && reportValidations(M_519_1_2)
      && reportValidations(M_519_1_3);
  }

  private boolean readXMLMetadataUserLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:roles/ns:role";

      String xpathExpressionUser = "/ns:siardArchive/ns:users/ns:user/ns:name";
      users = getXPathResult(zipFile, pathToEntry, xpathExpressionUser, XPathConstants.NODESET, null);

      String xpathExpressionRoles = "/ns:siardArchive/ns:roles/ns:role/ns:name";
      roles = getXPathResult(zipFile, pathToEntry, xpathExpressionRoles, XPathConstants.NODESET, null);

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element role = (Element) nodes.item(i);

        String name = MetadataXMLUtils.getChildTextContext(role, Constants.NAME);
        if (!validateRoleName(name))
          break;

        String admin = MetadataXMLUtils.getChildTextContext(role, ADMIN);
        if (!validateRoleAdmin(admin, name))
          break;

        String description = MetadataXMLUtils.getChildTextContext(role, Constants.DESCRIPTION);
        if (!validateRoleDescription(description, name))
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
  private boolean validateRoleName(String name) {
    if (!validateXMLField(M_519_1, name, Constants.NAME, true, false)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      addWarning(M_519_1_1, String.format("Role name %s should be unique", name));
    }
    return true;
  }

  /**
   * M_5.19-1-1 The role admin in SIARD file should exist. WARNING if it is not a
   * user or role
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoleAdmin(String admin, String name) {
    if (admin == null || admin.isEmpty()) {
      setError(M_519_1_2, String.format("Role admin inside %s is mandatory", name));
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

    addWarning(M_519_1_2, String.format("Admin %s inside role %s should be an existing user or role", admin, name));
    return true;
  }

  /**
   * M_5.19-1-2 The role description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoleDescription(String description, String name) {
    return validateXMLField(M_519_1_3, description, Constants.DESCRIPTION, false, true, Constants.USER, name);
  }
}
