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

  private Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();
  private List<Element> primaryKeyList = new ArrayList<>();

  public MetadataPrimaryKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_581, M_581_1, A_M_581_1, M_581_2, A_M_581_2, A_M_581_3);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_58);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_58, MODULE_NAME);

    validateMandatoryXSDFields(M_581, UNIQUE_KEY_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:primaryKey");

    if (!readXMLMetadataPrimaryKeyLevel()) {
      reportValidations(M_581, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (primaryKeyList.isEmpty()) {
      getValidationReporter().skipValidation(M_581, "Database has no primary keys");
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataPrimaryKeyLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

        Element tableColumnsElement = XMLUtils.getChild(tableElement, Constants.COLUMNS);
        if (tableColumnsElement == null) {
          return false;
        }
        NodeList tableColumns = tableColumnsElement.getElementsByTagName(Constants.COLUMN);

        LinkedList<String> tableColumnName = new LinkedList<>();
        for (int ci = 0; ci < tableColumns.getLength(); ci++) {
          tableColumnName.add(XMLUtils.getChildTextContext((Element) tableColumns.item(ci), Constants.NAME));
        }
        tableColumnsList.put(table, tableColumnName);

        NodeList primaryKeyNodes = tableElement.getElementsByTagName(Constants.PRIMARY_KEY);
        if (primaryKeyNodes == null) {
          // next table
          continue;
        }

        for (int j = 0; j < primaryKeyNodes.getLength(); j++) {
          Element primaryKey = (Element) primaryKeyNodes.item(j);
          String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.PRIMARY_KEY,
            Integer.toString(j));
          primaryKeyList.add(primaryKey);

          String name = XMLUtils.getChildTextContext(primaryKey, Constants.NAME);
          validatePrimaryKeyName(name, path);

          NodeList columns = primaryKey.getElementsByTagName(Constants.COLUMN);

          ArrayList<String> columnList = new ArrayList<>();
          for (int k = 0; k < columns.getLength(); k++) {
            String column = columns.item(k).getTextContent();
            columnList.add(column);
          }

          path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.PRIMARY_KEY, name);
          validatePrimaryKeyColumn(schema, table, columnList);

          String description = XMLUtils.getChildTextContext(primaryKey, Constants.DESCRIPTION);
          validatePrimaryKeyDescription(description, path);
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read primary key from SIARD file";
      setError(M_581, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }

  /**
   * M_5.8-1-1 The Primary Key name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.8-1-1 validation that primary key name not contain any blanks
   */
  private void validatePrimaryKeyName(String name, String path) {
    if (validateXMLField(M_581_1, name, Constants.PRIMARY_KEY, true, false, path)) {
      if (name.contains(BLANK)) {
        addWarning(A_M_581_1, "Primary key " + name + " contain blanks in name", path);
      }
      return;
    }
    setError(A_M_581_1, String.format("Aborted because primary key name is mandatory (%s)", path));
  }

  /**
   * M_5.8-1-2 The Primary Key column is mandatory in SIARD 2.1 specification
   *
   * A_M_5.8-1-2 The Primary Key column in SIARD file must exist on table. ERROR
   * if not exist
   */
  private void validatePrimaryKeyColumn(String schema, String table, List<String> columnList) {
    if (columnList.isEmpty()) {
      setError(M_581_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
      setError(A_M_581_2, String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
      return;
    }

    for (String column : columnList) {
      if (column.isEmpty()) {
        setError(M_581_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
        setError(A_M_581_2, String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
      } else if (!tableColumnsList.get(table).contains(column)) {
        setError(A_M_581_2, String.format("Primary key column reference %s not found on %s.%s", column, schema, table));
      }
    }
  }

  /**
   * A_M_5.8-1-3 The primary key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validatePrimaryKeyDescription(String description, String path) {
    validateXMLField(A_M_581_3, description, Constants.DESCRIPTION, false, true, path);
  }
}
