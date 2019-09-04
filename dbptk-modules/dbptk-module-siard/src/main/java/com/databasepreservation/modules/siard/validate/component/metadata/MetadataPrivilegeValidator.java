package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class MetadataPrivilegeValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataPrimaryKeyValidator.class);
  private final String MODULE_NAME;
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

  // databaseProduct
  private static final String COMMON = "common";
  private static final String MYSQL = "mysql";
  private static final String MS_ACCESS = "msaccess";
  private static final String OPEN_EDGE = "openedge";
  private static final String ORACLE = "oracle";
  private static final String POSTGRESQL = "postgresql";
  private static final String SQL_SERVER = "sqlserver";
  private static final String SYBASE = "sybase";

  // Common privileges types
  private static final String SELECT = "SELECT";
  private static final String INSERT = "INSERT";
  private static final String UPDATE = "UPDATE";
  private static final String DELETE = "DELETE";
  private static final String REFERENCES = "REFERENCES";
  private static final String USAGE = "USAGE";
  private static final String TRIGGER = "TRIGGER";
  private static final String ALL_PRIVILEGES = "ALL PRIVILEGES";
  private static final String INDEX = "INDEX";
  private static final String CREATE = "CREATE";
  private static final String ALTER = "ALTER";
  private static final String EXECUTE = "EXECUTE";

  // mysql privileges types
  private static final String ALL = "ALL";
  private static final String ALTER_ROUTINE = "ALTER ROUTINE";
  private static final String CREATE_ROLE = "CREATE ROLE";
  private static final String CREATE_ROUTINE = "CREATE ROUTINE";
  private static final String CREATE_TABLESPACE = "CREATE TABLESPACE";
  private static final String CREATE_TEMPORARY_TABLES = "CREATE TEMPORARY TABLES";
  private static final String CREATE_USER = "CREATE USER";
  private static final String CREATE_VIEW = "CREATE VIEW";
  private static final String DROP = "DROP";
  private static final String DROP_ROLE = "DROP ROLE";
  private static final String EVENT = "EVENT";
  private static final String FILE = "FILE";
  private static final String GRANT_OPTION = "GRANT OPTION";
  private static final String LOCK_TABLES = "LOCK TABLES";
  private static final String PROCESS = "PROCESS";
  private static final String PROXY = "PROXY";
  private static final String RELOAD = "RELOAD";
  private static final String REPLICATION_CLIENT = "REPLICATION CLIENT";
  private static final String REPLICATION_SLAVE = "REPLICATION SLAVE";
  private static final String SHOW_DATABASES = "SHOW DATABASES";
  private static final String SHOW_VIEW = "SHOW VIEW";
  private static final String SHUTDOWN = "SHUTDOWN";
  private static final String SUPER = "SUPER";

  // access privileges types
  private static final String OPEN = "Open";
  private static final String RUN = "Run";
  private static final String OPEN_EXCLUSIVE = "Open Exclusive";
  private static final String READ_DESIGN = "Read Design";
  private static final String MODIFY_DESIGN = "Modify Design";
  private static final String ADMINISTER = "Administer";
  private static final String READ_DATA = "Read Data";
  private static final String UPDATE_DATA = "Update Data";
  private static final String INSERT_DATA = "Insert Data";
  private static final String DELETE_DATA = "Delete Data";

  // oracle
  private static final String FLUSH = "FLUSH";
  private static final String LOAD = "LOAD";
  private static final String REFRESH = "REFRESH";
  private static final String UNLOAD = "UNLOAD";

  // postgress
  private static final String TRUNCATE = "TRUNCATE";
  private static final String CONNECT = "CONNECT";
  private static final String TEMPORARY = "TEMPORARY";
  private static final String TEMP = "TEMP";

  // sql server
  private static final String CONTROL = "CONTROL";
  private static final String RECEIVE = "RECEIVE";
  private static final String TAKE_OWNERSHIP = "TAKE OWNERSHIP";
  private static final String VIEW_CHANGE_TRACKING = "VIEW CHANGE TRACKING";
  private static final String VIEW_DEFINITION = "VIEW DEFINITION";

  private NodeList users;
  private NodeList roles;
  private List<String> objectPath = new ArrayList<>();
  private String databaseProduct = COMMON;
  private Map<String, List<String>> databaseProductList = new HashMap<>();

  public MetadataPrivilegeValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    // Common types
    databaseProductList.put(COMMON, new ArrayList<String>());
    databaseProductList.get(COMMON).add(SELECT);
    databaseProductList.get(COMMON).add(INSERT);
    databaseProductList.get(COMMON).add(UPDATE);
    databaseProductList.get(COMMON).add(DELETE);
    databaseProductList.get(COMMON).add(REFERENCES);
    databaseProductList.get(COMMON).add(USAGE);
    databaseProductList.get(COMMON).add(TRIGGER);
    databaseProductList.get(COMMON).add(ALL_PRIVILEGES);
    databaseProductList.get(COMMON).add(INDEX);
    databaseProductList.get(COMMON).add(CREATE);
    databaseProductList.get(COMMON).add(ALTER);
    databaseProductList.get(COMMON).add(EXECUTE);
    databaseProductList.get(COMMON).add(CREATE);
    databaseProductList.get(COMMON).add(ALL);

    // Mysql types
    databaseProductList.put(MYSQL, new ArrayList<String>());
    databaseProductList.get(MYSQL).add(ALTER_ROUTINE);
    databaseProductList.get(MYSQL).add(CREATE_ROLE);
    databaseProductList.get(MYSQL).add(CREATE_ROUTINE);
    databaseProductList.get(MYSQL).add(CREATE_TABLESPACE);
    databaseProductList.get(MYSQL).add(CREATE_TEMPORARY_TABLES);
    databaseProductList.get(MYSQL).add(CREATE_USER);
    databaseProductList.get(MYSQL).add(CREATE_VIEW);
    databaseProductList.get(MYSQL).add(DROP);
    databaseProductList.get(MYSQL).add(DROP_ROLE);
    databaseProductList.get(MYSQL).add(EVENT);
    databaseProductList.get(MYSQL).add(FILE);
    databaseProductList.get(MYSQL).add(GRANT_OPTION);
    databaseProductList.get(MYSQL).add(LOCK_TABLES);
    databaseProductList.get(MYSQL).add(PROCESS);
    databaseProductList.get(MYSQL).add(PROXY);
    databaseProductList.get(MYSQL).add(RELOAD);
    databaseProductList.get(MYSQL).add(REPLICATION_CLIENT);
    databaseProductList.get(MYSQL).add(REPLICATION_SLAVE);
    databaseProductList.get(MYSQL).add(SHOW_DATABASES);
    databaseProductList.get(MYSQL).add(SHOW_VIEW);
    databaseProductList.get(MYSQL).add(SHUTDOWN);
    databaseProductList.get(MYSQL).add(SUPER);

    // ms access
    databaseProductList.put(MS_ACCESS, new ArrayList<String>());
    databaseProductList.get(MS_ACCESS).add(OPEN);
    databaseProductList.get(MS_ACCESS).add(RUN);
    databaseProductList.get(MS_ACCESS).add(OPEN_EXCLUSIVE);
    databaseProductList.get(MS_ACCESS).add(READ_DESIGN);
    databaseProductList.get(MS_ACCESS).add(MODIFY_DESIGN);
    databaseProductList.get(MS_ACCESS).add(ADMINISTER);
    databaseProductList.get(MS_ACCESS).add(READ_DATA);
    databaseProductList.get(MS_ACCESS).add(UPDATE_DATA);
    databaseProductList.get(MS_ACCESS).add(INSERT_DATA);
    databaseProductList.get(MS_ACCESS).add(DELETE_DATA);

    // oracle
    databaseProductList.put(ORACLE, new ArrayList<String>());
    databaseProductList.get(ORACLE).add(FLUSH);
    databaseProductList.get(ORACLE).add(LOAD);
    databaseProductList.get(ORACLE).add(REFRESH);
    databaseProductList.get(ORACLE).add(UNLOAD);

    // postgress
    databaseProductList.put(POSTGRESQL, new ArrayList<String>());
    databaseProductList.get(POSTGRESQL).add(TRUNCATE);
    databaseProductList.get(POSTGRESQL).add(CONNECT);
    databaseProductList.get(POSTGRESQL).add(TEMPORARY);
    databaseProductList.get(POSTGRESQL).add(TEMP);

    // sql server
    databaseProductList.put(SQL_SERVER, new ArrayList<String>());
    databaseProductList.get(SQL_SERVER).add(CONTROL);
    databaseProductList.get(SQL_SERVER).add(RECEIVE);
    databaseProductList.get(SQL_SERVER).add(TAKE_OWNERSHIP);
    databaseProductList.get(SQL_SERVER).add(VIEW_CHANGE_TRACKING);
    databaseProductList.get(SQL_SERVER).add(VIEW_DEFINITION);

    databaseProductList.put(OPEN_EDGE, new ArrayList<String>());
    databaseProductList.put(SYBASE, new ArrayList<String>());

    setCodeListToValidate(M_520_1, M_520_1_1, M_520_1_2, M_520_1_3, M_520_1_4, M_520_1_5, M_520_1_6);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_520);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_520, MODULE_NAME);

    if (!validateMandatoryXSDFields(M_520_1, PRIVILEGE_TYPE, "/ns:siardArchive/ns:privileges/ns:privilege")) {
      reportValidations(M_520_1, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (!readXMLMetadataPrivilegeLevel()) {
      reportValidations(M_520_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(MODULE_NAME)) {
      metadataValidationPassed(MODULE_NAME);
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

      databaseProduct = getDatabaseProduct();

      String xpathExpressionRoles = "/ns:siardArchive/ns:roles/ns:role/ns:name";
      roles = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionRoles,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      // build list of objects ( SCHEMA.TABLE ) for use on validatePrivilegeObject
      String xpathExpressionObject = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";
      NodeList objectNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpressionObject,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < objectNodes.getLength(); i++) {
        Element tableElement = (Element) objectNodes.item(i);
        String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

        objectPath.add(schema + OBJECT_SEPARATOR + table);
      }

      String xpathExpression = "/ns:siardArchive/ns:privileges/ns:privilege";
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(pathToEntry), xpathExpression,
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element role = (Element) nodes.item(i);
        String path = buildPath(PRIVILEGE, Integer.toString(i));

        String type = XMLUtils.getChildTextContext(role, Constants.TYPE);
        if (!validatePrivilegeType(type, path))
          continue; // next privilege

        String object = XMLUtils.getChildTextContext(role, OBJECT);
        if (!validatePrivilegeObject(object, path))
          continue; // next privilege

        String grantor = XMLUtils.getChildTextContext(role, GRANTOR);
        if (!validatePrivilegeGrantor(grantor, path))
          continue; // next privilege

        String grantee = XMLUtils.getChildTextContext(role, GRANTEE);
        if (!validatePrivilegeGrantee(grantee, path))
          continue; // next privilege

        String option = XMLUtils.getChildTextContext(role, OPTION);
        if (!validatePrivilegeOption(option, path))
          continue; // next privilege

        String description = XMLUtils.getChildTextContext(role, Constants.DESCRIPTION);
        validatePrivilegeDescription(description, path);
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read privileges from SIARD file";
      setError(M_520_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  private String getDatabaseProduct() {
    try {
      String xpathExpression = "/ns:siardArchive/ns:databaseProduct/text()";
      String databaseProduct = ((String) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.STRING,
        Constants.NAMESPACE_FOR_METADATA)).replace(" ", "").toLowerCase();

      for (Map.Entry<String, List<String>> entry : databaseProductList.entrySet()) {
        if (databaseProduct.contains(entry.getKey())) {
          return entry.getKey();
        }
      }

      return COMMON;

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read database product from SIARD file";
      setError(M_520_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return null;
    }
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

    // Check type in common list
    for (String privilegeType : databaseProductList.get(COMMON)) {
      if (type.equals(privilegeType)) {
        return true;
      }
    }

    // if not found type in common list then check type for specific database
    // product
    if (databaseProductList.get(databaseProduct) != null) {
      for (String privilegeType : databaseProductList.get(databaseProduct)) {
        if (type.equals(privilegeType)) {
          return true;
        }
      }
    }

    addNotice(M_520_1_1, String.format("Privilege type '%s' is not a common type", type), privilegeNode);
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
      addWarning(M_520_1_2, String.format("Privilege object '%s' does not exist on database", path), privilegeNode);
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
  private void validatePrivilegeDescription(String description, String privilegeNode) {
    validateXMLField(M_520_1_6, description, Constants.DESCRIPTION, false, true, privilegeNode);
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
