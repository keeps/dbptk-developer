/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataPrimaryKeyValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataPrimaryKeyValidator.class);
  private final String MODULE_NAME;
  private static final String M_58 = "5.8";
  private static final String M_581 = "M_5.8-1";
  private static final String M_581_1 = "M_5.8-1-1";
  private static final String A_M_581_1 = "A_M_5.8-1-1";
  private static final String M_581_2 = "M_5.8-1-2";
  private static final String A_M_581_2 = "A_M_5.8-1-2";
  private static final String A_M_581_3 = "A_M_5.8-1-3";
  private static final String BLANK = " ";
  private boolean additionalCheckError = false;

  public MetadataPrimaryKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_58);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_58, MODULE_NAME);

    NodeList tables;
    NodeList keys;
    try {
      tables = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      keys = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:primaryKey", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read primary key from SIARD file";
      setError(M_581, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (keys.getLength() == 0) {
      getValidationReporter().skipValidation(M_581, "Database has no primary keys");
      observer.notifyValidationStep(MODULE_NAME, M_581, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_581, UNIQUE_KEY_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:primaryKey");

    if (validatePrimaryKeyName(keys)) {
      validationOk(MODULE_NAME, M_581_1);
      validationOk(MODULE_NAME, A_M_581_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_581_1, ValidationReporterStatus.ERROR);
      observer.notifyValidationStep(MODULE_NAME, A_M_581_1, ValidationReporterStatus.ERROR);
    }

    if (validatePrimaryKeyColumn(tables, keys)) {
      validationOk(MODULE_NAME, M_581_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_581_2, ValidationReporterStatus.ERROR);
    }

    if (!additionalCheckError) {
      validationOk(MODULE_NAME, A_M_581_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_581_2, ValidationReporterStatus.ERROR);
    }

    validatePrimaryKeyDescription(keys);
    validationOk(MODULE_NAME, A_M_581_3);

    closeZipFile();

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.8-1-1 The Primary Key name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.8-1-1 validation that primary key name not contain any blanks
   */
  private boolean validatePrimaryKeyName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element candidateKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(candidateKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(candidateKey, Constants.TABLE), Constants.CANDIDATE_KEY,
        Integer.toString(i));

      String name = XMLUtils.getChildTextContext(candidateKey, Constants.NAME);

      if (validateXMLField(M_581_1, name, Constants.CANDIDATE_KEY, true, false, path)) {
        if (name.contains(BLANK)) {
          addWarning(A_M_581_1, "Primary key " + name + " contain blanks in name", path);
        }
        continue;
      }
      setError(A_M_581_1, String.format("Aborted because primary key name is mandatory (%s)", path));
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.8-1-2 The Primary Key column is mandatory in SIARD 2.1 specification
   *
   * A_M_5.8-1-2 The Primary Key column in SIARD file must exist on table. ERROR
   * if not exist
   */
  private boolean validatePrimaryKeyColumn(NodeList tablesNodes, NodeList nodes) {
    boolean hasErrors = false;
    Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();
    for (int i = 0; i < tablesNodes.getLength(); i++) {
      Element tableElement = (Element) tablesNodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);

      Element tableColumnsElement = XMLUtils.getChild(tableElement, Constants.COLUMNS);
      if (tableColumnsElement == null) {
        return false;
      }
      NodeList tableColumns = tableColumnsElement.getElementsByTagName(Constants.COLUMN);
      LinkedList<String> tableColumnName = new LinkedList<>();

      for (int j = 0; j < tableColumns.getLength(); j++) {
        tableColumnName.add(XMLUtils.getChildTextContext((Element) tableColumns.item(j), Constants.NAME));
      }
      tableColumnsList.put(table, tableColumnName);
    }

    for (int i = 0; i < nodes.getLength(); i++) {
      Element primaryKey = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(primaryKey, Constants.SCHEMA);
      String table = XMLUtils.getParentNameByTagName(primaryKey, Constants.TABLE);
      NodeList columns = primaryKey.getElementsByTagName(Constants.COLUMN);

      ArrayList<String> columnList = new ArrayList<>();
      for (int k = 0; k < columns.getLength(); k++) {
        String column = columns.item(k).getTextContent();
        columnList.add(column);
      }

      if (columnList.isEmpty()) {
        setError(M_581_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
        setError(A_M_581_2, String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
        additionalCheckError = true;
        hasErrors = true;
        continue;
      }

      for (String column : columnList) {
        if (column.isEmpty()) {
          setError(M_581_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
          setError(A_M_581_2, String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
          hasErrors = true;
          additionalCheckError = true;
        } else if (!tableColumnsList.get(table).contains(column)) {
          setError(A_M_581_2,
            String.format("Primary key column reference %s not found on %s.%s", column, schema, table));
          additionalCheckError = true;
        }
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.8-1-3 The primary key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validatePrimaryKeyDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element primaryKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(primaryKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(primaryKey, Constants.TABLE), Constants.CANDIDATE_KEY,
        XMLUtils.getChildTextContext(primaryKey, Constants.NAME));

      String description = XMLUtils.getChildTextContext(primaryKey, Constants.DESCRIPTION);
      validateXMLField(A_M_581_3, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
