package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class MetadataPrivilegeValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Privilege level metadata";
  private static final String M_520 = "5.20";
  private static final String M_520_1 = "M_5.20-1";
  private static final String M_520_1_1 = "M_5.20-1-1";
  private static final String M_520_1_2 = "M_5.20-1-2";
  private static final String M_520_1_3 = "M_5.20-1-3";
  private static final String M_520_1_4 = "M_5.20-1-4";
  private static final String M_520_1_5 = "M_5.20-1-5";
  private static final String M_520_1_6 = "M_5.20-1-6";

  private static final String PRIVILEGE = "privilege";
  private static final String OBJECT = "object";
  private static final String OBJECT_SEPARATOR = ".";
  private static final String OBJECT_SPACE = " ";
  private static final String GRANTOR = "grantor";
  private static final String GRANTEE = "grantee";
  private static final String OPTION = "option";
  private static final String OPTION_ADMIN = "ADMIN";
  private static final String OPTION_GRANT = "GRANT";

  // Privileges types
  private static final String TYPE_SELECT = "SELECT";
  private static final String TYPE_INSERT = "INSERT";
  private static final String TYPE_UPDATE = "UPDATE";
  private static final String TYPE_DELETE = "DELETE";
  private static final String TYPE_REFERENCES = "REFERENCES";
  private static final String TYPE_USAGE = "USAGE";
  private static final String TYPE_TRIGGER = "TRIGGER";

  private NodeList users;
  private NodeList roles;
  private List<String> objectPath = new ArrayList<>();

  public static MetadataPrivilegeValidator newInstance() {
    return new MetadataPrivilegeValidator();
  }

  private MetadataPrivilegeValidator() {

  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_520, MODULE_NAME);

    if (!readXMLMetadataPrivilegeLevel()) {
      reportValidations(M_520_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_520_1, MODULE_NAME) && reportValidations(M_520_1_1, MODULE_NAME)
      && reportValidations(M_520_1_2, MODULE_NAME) && reportValidations(M_520_1_3, MODULE_NAME)
      && reportValidations(M_520_1_4, MODULE_NAME) && reportValidations(M_520_1_5, MODULE_NAME)
      && reportValidations(M_520_1_6, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataPrivilegeLevel() {
    try {
      String pathToEntry = validatorPathStrategy.getMetadataXMLPath();

      String xpathExpressionUser = "/ns:siardArchive/ns:users/ns:user/ns:name";
      users = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionUser,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      String xpathExpressionRoles = "/ns:siardArchive/ns:roles/ns:role/ns:name";
      roles = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionRoles,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      // build list of objects ( SCHEMA.TABLE ) for use on validatePrivilegeObject
      String xpathExpressionObject = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";
      NodeList objectNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionObject,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < objectNodes.getLength(); i++) {
        Element tableElement = (Element) objectNodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = MetadataXMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

        objectPath.add(schema + OBJECT_SEPARATOR + table);
      }

      String xpathExpression = "/ns:siardArchive/ns:privileges/ns:privilege";
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpression,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element role = (Element) nodes.item(i);
        String privilegeNode = String.format("Privilege Node[%d]", i);
        String path = buildPath(PRIVILEGE, privilegeNode);

        String type = MetadataXMLUtils.getChildTextContext(role, Constants.TYPE);
        if (!validatePrivilegeType(type, path))
          break;

        String object = MetadataXMLUtils.getChildTextContext(role, OBJECT);
        if (!validatePrivilegeObject(object, path))
          break;

        String grantor = MetadataXMLUtils.getChildTextContext(role, GRANTOR);
        if (!validatePrivilegeGrantor(grantor, path))
          break;

        String grantee = MetadataXMLUtils.getChildTextContext(role, GRANTEE);
        if (!validatePrivilegeGrantee(grantee, path))
          break;

        String option = MetadataXMLUtils.getChildTextContext(role, OPTION);
        if (!validatePrivilegeOption(option, path))
          break;

        String description = MetadataXMLUtils.getChildTextContext(role, Constants.DESCRIPTION);
        if (!validatePrivilegeDescription(description, path))
          break;
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.20-1-1 The privilege type field in SIARD should be a common DBMS type,
   * ERROR if it is empty, WARNING if it is not a common type
   * 
   * @return true if valid otherwise false
   */
  private boolean validatePrivilegeType(String type, String privilegeNode) {
    if (!validateXMLField(M_520_1_1, type, Constants.TYPE, true, false, privilegeNode)) {
      return false;
    }
    switch (type) {
      case TYPE_SELECT:
      case TYPE_INSERT:
      case TYPE_UPDATE:
      case TYPE_DELETE:
      case TYPE_REFERENCES:
      case TYPE_USAGE:
      case TYPE_TRIGGER:
        break;
      default:
        addNotice(M_520_1_1,
          String.format("Privilege type '%s' inside '%s' is not a common type", type, privilegeNode));
    }
    return true;
  }

  /**
   * M_5.20-1-2 The privilege object field in SIARD should be an existing object
   * in database. WARNING if object does not exist on database
   */
  private boolean validatePrivilegeObject(String object, String privilegeNode) {
    String path;
    if (object.contains(OBJECT_SPACE)) {
      path = object.split(OBJECT_SPACE)[1].replace("\"", "");
    } else {
      path = object;
    }

    if (!objectPath.contains(path)) {
      addWarning(M_520_1_2, String.format("Privilege object '%s' not exist on database", path), privilegeNode);
    }

    return true;
  }

  /**
   * M_5.20-1-3 The privilege grantor field in SIARD should be an existing user or
   * role. ERROR if it is empty, WARNING it not exist user or role
   *
   * @return true if valid otherwise false
   */
  private boolean validatePrivilegeGrantor(String grantor, String privilegeNode) {
    if (!validateXMLField(M_520_1_3, grantor, GRANTOR, true, false, privilegeNode)) {
      return false;
    }
    if (!checkIfUserOrRoleExist(grantor)) {
      addWarning(M_520_1_3, String.format("Grantor %s should be an existing user or role", grantor), privilegeNode);
    }
    return true;
  }

  /**
   * M_5.20-1-4 The privilege grantee field in SIARD should be an existing user or
   * role. ERROR if it is empty, WARNING it not exist user or role
   * 
   * @return true if valid otherwise false
   */
  private boolean validatePrivilegeGrantee(String grantee, String privilegeNode) {
    if (!validateXMLField(M_520_1_4, grantee, GRANTOR, true, false, privilegeNode)) {
      return false;
    }
    if (!checkIfUserOrRoleExist(grantee)) {
      addWarning(M_520_1_4, String.format("Grantee %s should be an existing user or role", grantee), privilegeNode);
    }
    return true;
  }

  /**
   * M_5.20-1-5 The privilege option field in SIARD should be 'ADMIN', 'GRANT' or
   * empty.
   */
  private boolean validatePrivilegeOption(String option, String privilegeNode) {
    if (option != null && !option.isEmpty()) {
      switch (option) {
        case OPTION_ADMIN:
        case OPTION_GRANT:
          break;
        default:
          addWarning(M_520_1_5,
            String.format("option should be %s, %s or empty. Found '%s'", OPTION_ADMIN, OPTION_GRANT, option),
            privilegeNode);
      }
    }
    return true;
  }

  /**
   * M_5.20-1-6 The privilege description field in SIARD file should not be less
   * than 3 characters. WARNING if it is less than 3 characters
   */
  private boolean validatePrivilegeDescription(String description, String privilegeNode) {
    return validateXMLField(M_520_1_6, description, Constants.DESCRIPTION, false, true, privilegeNode);
  }

  private boolean checkIfUserOrRoleExist(String name) {
    for (int i = 0; i < users.getLength(); i++) {
      if (users.item(i).getTextContent().equals(name)) {
        return true;
      }
    }

    for (int i = 0; i < roles.getLength(); i++) {
      if (roles.item(i).getTextContent().equals(name)) {
        return true;
      }
    }

    return false;
  }
}
