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
import java.util.ArrayList;
import java.util.Collections;
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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.bindings.siard_2_1.SiardArchive;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
abstract class MetadataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataValidator.class);
  private static final int MIN_FIELD_LENGTH = 3;
  private static final String ENTRY = "metadata.xml";
  static final String SIARD_ARCHIVE = "siardArchive";
  static final String SCHEMA_TYPE = "schemaType";
  static final String TYPE_TYPE = "typeType";
  static final String ATTRIBUTE_TYPE = "attributeType";
  static final String TABLE_TYPE = "tableType";
  static final String COLUMN_TYPE = "columnType";
  static final String FIELD_TYPE = "fieldType";
  static final String UNIQUE_KEY_TYPE = "uniqueKeyType";
  static final String FOREIGN_KEYS_TYPE = "foreignKeyType";
  static final String REFERENCE_TYPE = "referenceType";
  static final String CHECK_CONSTRAINT_TYPE = "checkConstraintType";
  static final String TRIGGER_TYPE = "triggerType";
  static final String VIEW_TYPE = "viewType";
  static final String ROUTINE_TYPE = "routineType";
  static final String PARAMETER_TYPE = "parameterType";
  static final String USER_TYPE = "userType";
  static final String ROLE_TYPE = "roleType";
  static final String PRIVILEGE_TYPE = "privilegeType";

  private List<String> codeList = new ArrayList<>();
  private Map<String, List<Map<String, String>>> warnings = new HashMap<>();
  private Map<String, List<Map<String, String>>> notices = new HashMap<>();
  private Map<String, List<String>> errors = new HashMap<>();
  private boolean failed = false;

  void setCodeListToValidate(String... codeIDList) {
    Collections.addAll(codeList, codeIDList);
  }

  boolean reportValidations(String moduleName) {
    for (String codeId : codeList) {
      reportValidations(codeId, moduleName);
    }
    if (failed) {
      observer.notifyFinishValidationModule(moduleName, ValidationReporterStatus.FAILED);
      getValidationReporter().moduleValidatorFinished(moduleName, ValidationReporterStatus.ERROR);
      return false;
    }
    metadataValidationPassed(moduleName);
    return true;
  }

  /**
   * search through the codeID for errors, warnings or notice, if found write on
   * reporter and notify the observer
   */
  void reportValidations(String codeID, String moduleName) {
    boolean codeIdFailed = false;
    if (errors.get(codeID) != null && !errors.get(codeID).isEmpty()) {
      for (String error : errors.get(codeID)) {
        getValidationReporter().validationStatus(codeID, ValidationReporterStatus.ERROR, error);
        codeIdFailed = failed = true;
      }
    } else if ((warnings.get(codeID) != null) && !warnings.get(codeID).isEmpty()) {
      for (Map<String, String> entry : warnings.get(codeID)) {
        for (Map.Entry<String, String> warning : entry.entrySet()) {
          getValidationReporter().warning(codeID, warning.getKey(), warning.getValue());
        }
      }
    } else if (notices.get(codeID) != null) {
      for (Map<String, String> entry : notices.get(codeID)) {
        for (Map.Entry<String, String> notice : entry.entrySet()) {
          getValidationReporter().notice(notice.getValue(), notice.getKey());
        }
      }
    }

    if (codeIdFailed) {
      observer.notifyValidationStep(moduleName, codeID, ValidationReporterStatus.ERROR);
    } else {
      observer.notifyValidationStep(moduleName, codeID, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(codeID, ValidationReporterStatus.OK);
    }
  }

  void metadataValidationPassed(String moduleName) {
    getValidationReporter().moduleValidatorFinished(moduleName, ValidationReporterStatus.PASSED);
    observer.notifyFinishValidationModule(moduleName, ValidationReporterStatus.PASSED);
  }

  /**
   * Common validator for mandatory and optionals fields
   *
   * @param codeId
   *          defined by Estonian National Archive
   * @param value
   *          value to validate
   * @param field
   *          field name for display in messages
   * @param mandatory
   *          if value is required in SIARD specification or if was defined by
   *          Estonian National Archive
   * @param checkSize
   *          check if field size is smaller than MIN_FIELD_LENGTH (defined by
   *          Estonian National Archive)
   * @param path
   *          XML path to assist error and warning messages
   * @return true if valid otherwise false
   */
  boolean validateXMLField(String codeId, String value, String field, Boolean mandatory, Boolean checkSize,
    String path) {
    if (!validateMandatoryXMLField(value) && mandatory) {
      setError(codeId, buildErrorMessage(field, path));
      return false;
    } else if (!validateXMLFieldSize(value) && checkSize) {
      addWarning(codeId, buildWarningMessage(field, value), path);
    }
    return true;
  }

  /**
   * For mandatory fields in metadata, the value must exist and not be empty
   * 
   * @return true if valid otherwise false
   */
  private boolean validateMandatoryXMLField(String value) {
    return value != null && !value.trim().isEmpty();
  }

  /**
   * For mandatory and optional fields in metadata, check if field size is smaller
   * than MIN_FIELD_LENGTH (defined by Estonian National Archive)
   * 
   * @return true if valid otherwise false
   */
  private boolean validateXMLFieldSize(String value) {
    return value != null && !value.trim().isEmpty() && value.length() >= MIN_FIELD_LENGTH;
  }

  /**
   * Commons warning messages for mandatory and optionals fields
   * 
   * @return warning message with XML path or null if not exist any warning
   */
  private String buildWarningMessage(String field, String value) {
    if (value == null) {
      return String.format("The %s element not exist", field);
    } else if (value.isEmpty()) {
      return String.format("The %s is empty", field);
    } else if (value.trim().isEmpty()) {
      return String.format("The %s contains only spaces", field);
    } else if (!validateXMLFieldSize(value)) {
      return String.format("The %s '%s' has less than %d characters", field, value, MIN_FIELD_LENGTH);
    }
    return null;
  }

  /**
   * Common error message for mandatory fields
   * 
   * @return error message with XML path
   */
  public String buildErrorMessage(String field, String path) {
    return String.format("The %s is mandatory inside %s", field, path);
  }

  /**
   * Build a path to assist error and warning messages
   * 
   * @return XML path
   */
  protected String buildPath(String... parameters) {
    StringBuilder path = new StringBuilder();
    path.append(ENTRY).append(" ");
    for (int i = 0; i < parameters.length; i++) {
      path.append(parameters[i]);
      if (i % 2 != 0 && i < parameters.length - 1) {
        path.append("/");
      } else {
        path.append(":");
      }
    }
    path.deleteCharAt(path.length() - 1);

    return path.toString();
  }

  void addWarning(String codeID, String message, String path) {
    addMessageList(codeID, message, path, warnings);
  }

  void addNotice(String codeID, String message, String path) {
    addMessageList(codeID, message, path, notices);
  }

  private void addMessageList(String codeID, String message, String path,
    Map<String, List<Map<String, String>>> typeMessage) {
    if (typeMessage.get(codeID) == null) {
      typeMessage.put(codeID, new ArrayList<Map<String, String>>());
    }
    Map<String, String> messageObject = new HashMap<>();
    messageObject.put(path, message);
    typeMessage.get(codeID).add(messageObject);
  }

  void setError(String codeID, String error) {
    if (errors.get(codeID) == null) {
      errors.put(codeID, new ArrayList<String>());
    }
    this.errors.get(codeID).add(error);
  }

  static String createPath(String... parameters) {
    StringBuilder sb = new StringBuilder();
    for (String parameter : parameters) {
      sb.append(parameter).append("/");
    }
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }

  /**
   * All metadata that are designated as mandatory in metadata.xsd at database
   * level must be completed accordingly
   * 
   * @param codeID
   *          defined by Estonian National Archive
   * @param level
   *          defined in metadata.xsd
   * @param xmlExpresion
   *          path to metadata.xml object
   * @return true if valid otherwise false
   */
  boolean validateMandatoryXSDFields(String codeID, String level, String xmlExpresion) {
    if (preValidationRequirements())
      return false;
    try {
      String xsdExpression;
      if (level.equals(SIARD_ARCHIVE)) {
        xsdExpression = "/xs:schema/xs:element[@name='siardArchive']/xs:complexType/xs:sequence/xs:element[not(@minOccurs='0')]";
      } else {
        xsdExpression = "/xs:schema/xs:complexType[@name='" + level + "']/xs:sequence/xs:element[not(@minOccurs='0')]";
      }

      final InputStream XSDInputStream = SiardArchive.class.getClassLoader()
        .getResourceAsStream("schema/siard2-1-metadata.xsd");

      NodeList nodes = (NodeList) XMLUtils.getXPathResult(XSDInputStream, xsdExpression, XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element element = (Element) nodes.item(i);
        String mandatoryItemName = element.getAttribute(Constants.NAME);

        NodeList nodesToValidate = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xmlExpresion, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        if(nodesToValidate.getLength() == 0){
          setError(codeID, String.format("Nothing found in this path %s", xmlExpresion));
          return false;
        }
        for (int j = 0; j < nodesToValidate.getLength(); j++) {
          Element itemToValidate = XMLUtils.getChild((Element) nodesToValidate.item(j), mandatoryItemName);
          if (itemToValidate == null) {
            setError(codeID, String.format("Mandatory item '%s' must be set (%s)", mandatoryItemName, xmlExpresion));
          }
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read metadata.xsd file";
      setError(codeID, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }
}
