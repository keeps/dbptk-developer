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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.common.model.SQLType;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataReferenceValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataReferenceValidator.class);
  private final String MODULE_NAME;
  private static final String M_510 = "5.10";
  private static final String M_510_1 = "M_5.10-1";
  private static final String M_510_1_1 = "M_5.10-1-1";
  private static final String A_M_510_1_1 = "A_M_5.10-1-1";
  private static final String M_510_1_2 = "M_5.10-1-2";
  private static final String A_M_510_1_2 = "A_M_5.10-1-2";

  private static final String REFERENCE = "reference";
  private static final String REFERENCED_TABLE = "referencedTable";
  private static final String REFERENCED_SCHEMA = "referencedSchema";
  private static final String REFERENCED_COLUMN = "referenced";

  private List<Element> foreignKeyList = new ArrayList<>();
  private List<String> tableList = new ArrayList<>();
  private List<SQLType> SQLTypeList = null;
  private Map<String, HashMap<String, String>> tableColumnsList = new HashMap<>();
  private Map<String, List<String>> primaryKeyList = new HashMap<>();
  private Map<String, List<String>> candidateKeyList = new HashMap<>();
  private boolean additionalCheckError = false;

  public MetadataReferenceValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    populateSQL2008Types();
  }

  @Override
  public void clean() {
    foreignKeyList.clear();
    tableList.clear();
    SQLTypeList.clear();
    tableColumnsList.clear();
    primaryKeyList.clear();
    candidateKeyList.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_510);
    getValidationReporter().moduleValidatorHeader(M_510, MODULE_NAME);

    NodeList references;
    NodeList nodes;

    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
      InputStream referencesInputStream = zipFileManagerStrategy.getZipInputStream(path,
        validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      references = (NodeList) XMLUtils.getXPathResult(referencesInputStream,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:foreignKeys/ns:foreignKey/ns:reference",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      tableColumnsList = getListColumnsByTables(nodes);
      primaryKeyList = getListKeysByTables(nodes, Constants.PRIMARY_KEY);
      candidateKeyList = getListKeysByTables(nodes, Constants.CANDIDATE_KEY);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        NodeList foreignKeyNodes = tableElement.getElementsByTagName(Constants.FOREIGN_KEY);
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKeyElement = (Element) foreignKeyNodes.item(j);
          foreignKeyList.add(foreignKeyElement);
        }

      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read references from SIARD file";
      setError(M_510_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (foreignKeyList.isEmpty()) {
      getValidationReporter().skipValidation(M_510, "Database has no foreign keys and references");
      observer.notifyValidationStep(MODULE_NAME, M_510, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_510_1, REFERENCE_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:foreignKeys/ns:foreignKey/ns:reference");

    if (validateColumn(references)) {
      validationOk(MODULE_NAME, M_510_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_510_1_1, ValidationReporterStatus.ERROR);
    }

    // A_M_510_1_1
    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_510_1_1, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_510_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_510_1_1);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_510_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateReferencedColumn(nodes)) {
      validationOk(MODULE_NAME, M_510_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_510_1_2, ValidationReporterStatus.ERROR);
    }

    // A_M_510_1_2
    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_510_1_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_510_1_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_510_1_2);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_510_1_2, ValidationReporterStatus.ERROR);
      }
    }

    return reportValidations(MODULE_NAME);
  }

  private Map<String, HashMap<String, String>> getListColumnsByTables(NodeList tableNodes) {
    Map<String, HashMap<String, String>> columnsTables = new HashMap<>();

    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element tableElement = (Element) tableNodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
      String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

      Element tableColumnsElement = XMLUtils.getChild(tableElement, Constants.COLUMNS);
      if (tableColumnsElement == null) {
        break;
      }

      NodeList columnNode = tableColumnsElement.getElementsByTagName(Constants.COLUMN);
      HashMap<String, String> columnsNameList = new HashMap<>();
      for (int j = 0; j < columnNode.getLength(); j++) {
        Element columnElement = (Element) columnNode.item(j);
        String name = XMLUtils.getChildTextContext(columnElement, Constants.NAME);
        String type = XMLUtils.getChildTextContext(columnElement, Constants.TYPE);
        columnsNameList.put(name, type);
      }
      columnsTables.put(schema + table, columnsNameList);
    }
    return columnsTables;
  }

  private Map<String, List<String>> getListKeysByTables(NodeList tableNodes, String key) {
    Map<String, List<String>> keys = new HashMap<>();

    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element tableElement = (Element) tableNodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);

      NodeList keyNodes = tableElement.getElementsByTagName(key);
      if (keyNodes == null) {
        continue;
      }

      List<String> columnsNameList = new ArrayList<>();
      for (int j = 0; j < keyNodes.getLength(); j++) {
        Element keyElement = (Element) keyNodes.item(j);
        NodeList columnNode = keyElement.getElementsByTagName(Constants.COLUMN);
        for (int k = 0; k < columnNode.getLength(); k++) {
          String name = columnNode.item(k).getTextContent();
          columnsNameList.add(name);
        }
      }
      keys.put(table, columnsNameList);
    }

    return keys;
  }

  /**
   * M_5.10-1-1 The column in reference is mandatory in SIARD 2.1 specification
   *
   * A_M_5.10-1-1 The column in reference must exist on table. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumn(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element reference = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(reference, Constants.SCHEMA);
      String table = XMLUtils.getParentNameByTagName(reference, Constants.TABLE);
      String foreignKey = XMLUtils.getParentNameByTagName(reference, Constants.FOREIGN_KEY);

      String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY, foreignKey,
        Constants.FOREIGN_KEY_REFERENCE, Integer.toString(i));

      String column = XMLUtils.getChildTextContext(reference, Constants.COLUMN);

      if (validateXMLField(M_510_1_1, column, Constants.COLUMN, true, false, path)) {
        if (tableColumnsList.get(schema + table).get(column) == null && !this.skipAdditionalChecks) {
          setError(A_M_510_1_1,
            String.format("referenced column name %s does not exist on referenced table %s", column, table));
          additionalCheckError = true;
          continue;
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_510_1_1, String.format("Aborted because referenced column name is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.10-1-2 The referenced column is mandatory in SIARD 2.1 specification
   *
   * A_M_5.10-1-2: Validation that fk and reference table pk have identical data
   * types
   *
   * @return true if valid otherwise false
   */
  private boolean validateReferencedColumn(NodeList nodes) {
    boolean hasErrors = false;
    additionalCheckError = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element tableElement = (Element) nodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
      String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);
      String foreignKeyTable = table;
      tableList.add(table);

      NodeList foreignKeyNodes = tableElement.getElementsByTagName(Constants.FOREIGN_KEY);
      for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
        Element foreignKeyElement = (Element) foreignKeyNodes.item(j);
        foreignKeyList.add(foreignKeyElement);
        String foreignKey = XMLUtils.getChildTextContext(foreignKeyElement, Constants.NAME);
        String referencedTable = XMLUtils.getChildTextContext(foreignKeyElement, REFERENCED_TABLE);
        String referencedSchema = XMLUtils.getChildTextContext(foreignKeyElement, REFERENCED_SCHEMA);
        NodeList referenceNodes = foreignKeyElement.getElementsByTagName(REFERENCE);
        for (int k = 0; k < referenceNodes.getLength(); k++) {
          Element reference = (Element) referenceNodes.item(k);
          String column = XMLUtils.getChildTextContext(reference, Constants.COLUMN);

          String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY, foreignKey,
            Constants.FOREIGN_KEY_REFERENCE, column);

          if (column == null || column.isEmpty()) {
            setError(M_510_1_2, String.format("Unable to validate, column does not exist (%s)", path));
            if (!this.skipAdditionalChecks) {
              setError(A_M_510_1_2, String.format("Unable to validate, column does not exist (%s)", path));
              additionalCheckError = true;
            }
            hasErrors = true;
            continue;
          }

          if (tableColumnsList.get(schema + table).get(column) == null) {
            setError(M_510_1_2, String.format("Unable to validate, column does not exist (%s)", path));
            if (!this.skipAdditionalChecks) {
              setError(A_M_510_1_2, String.format("Unable to validate, column does not exist (%s)", path));
              additionalCheckError = true;
            }
            hasErrors = true;
            continue;
          }

          String referencedColumn = XMLUtils.getChildTextContext(reference, REFERENCED_COLUMN);
          HashMap<String, String> referencedColumnTable = new HashMap<>();
          HashMap<String, String> foreignKeyColumnTable = new HashMap<>();
          List<String> primaryKeyColumns = new ArrayList<>();
          List<String> candidateKeyColumns = new ArrayList<>();

          if (!validateXMLField(M_510_1_2, referencedColumn, REFERENCED_COLUMN, true, false, path)) {
            setError(A_M_510_1_2, String.format("Unable to validate, referenced does not exist (%s)", path));
            hasErrors = true;
            additionalCheckError = true;
            continue;
          }

          if (!this.skipAdditionalChecks) {
            if (referencedTable == null || tableColumnsList.get(referencedSchema + referencedTable) == null) {
              setError(A_M_510_1_2, String.format("Unable to validate, referenced table does not exist (%s)", path));
              additionalCheckError = true;
              continue;
            }

            if (foreignKeyTable == null || tableColumnsList.get(schema + foreignKeyTable) == null) {
              setError(A_M_510_1_2, String.format("Unable to validate, foreign key table does not exist (%s)", path));
              additionalCheckError = true;
              continue;
            }

            referencedColumnTable = tableColumnsList.get(referencedSchema + referencedTable);
            foreignKeyColumnTable = tableColumnsList.get(schema + foreignKeyTable);
            primaryKeyColumns = primaryKeyList.get(referencedTable);
            candidateKeyColumns = candidateKeyList.get(referencedTable);

            // M_5.10-1-2
            if (referencedColumnTable.get(referencedColumn) == null) {
              setError(A_M_510_1_2,
                String.format("referenced column name %s of table %s does not exist on referenced table %s",
                  referencedColumn, foreignKeyTable, referencedTable));
              additionalCheckError = true;
              continue;
            }

            // Additional check
            String foreignKeyType = foreignKeyColumnTable.get(column);
            if (primaryKeyColumns != null && primaryKeyColumns.contains(referencedColumn)) {
              for (String primaryKey : primaryKeyColumns) {
                if (!primaryKey.equals(referencedColumn))
                  continue;
                String primaryKeyType = referencedColumnTable.get(primaryKey);

                if (primaryKeyType == null) {
                  setError(A_M_510_1_2,
                    String.format("Unable to find primary key type referencedTable:%s/primaryKey:%s in %s",
                      referencedTable, primaryKey, path));
                  additionalCheckError = true;
                  break;
                }

                if (!checkType(foreignKeyType, primaryKeyType)) {
                  setError(A_M_510_1_2,
                    String.format("Foreign Key %s.%s type %s does not match with type %s of Primary Key %s.%s",
                      foreignKeyTable, foreignKey, foreignKeyType, primaryKeyType, referencedTable, primaryKey));
                  additionalCheckError = true;
                  break;
                }
              }
            }
          }
        }
      }
    }

    return !hasErrors;
  }

  /**
   * * Checks whether the foreign key type is compatible with the referenced key
   * type
   *
   * The types are separated into groups and are only compatible if the groups are
   * equal and the size of the referenced key is greater than or equal to that of
   * the foreign key.
   * 
   * @return true when compatible otherwise false
   */
  private boolean checkType(String foreignNameType, String referencedType) {

    SQLType foreignKeyType = findTypeInList(foreignNameType);
    SQLType primaryKeyType = findTypeInList(referencedType);

    if (foreignKeyType == null || primaryKeyType == null) {
      return false;
    }

    if (!foreignKeyType.getGroup().equals(primaryKeyType.getGroup())) {
      return false;
    }

    return foreignKeyType.checkSize(primaryKeyType);
  }

  private SQLType findTypeInList(String typeName) {
    SQLType typeToCompare = null;
    for (SQLType type : SQLTypeList) {
      if (typeName.matches(type.getPattern())) {
        typeToCompare = new SQLType(type.getGroup(), type.getPattern(), type.getSize(), typeName);
        break;
      }
    }
    return typeToCompare;
  }

  private void populateSQL2008Types() {
    SQLTypeList = new ArrayList<>();

    // Character strings
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^VARCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList
      .add(new SQLType(SQLType.Group.CHARACTER, "^NATIONAL\\s+CHARACTER\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList
      .add(new SQLType(SQLType.Group.CHARACTER, "^NATIONAL\\s+CHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^NCHAR\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^NATIONAL\\s+CHARACTER(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^NATIONAL\\s+CHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.CHARACTER, "^NCHAR(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    // Binary strings
    SQLTypeList.add(new SQLType(SQLType.Group.BINARY, "^BINARY VARYING\\(\\d+\\)$"));
    SQLTypeList.add(new SQLType(SQLType.Group.BINARY, "^BINARY\\s+VARYING(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.BINARY, "^VARBINARY(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    // Numbers
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^BIGINT$", 64));
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^DOUBLE PRECISION$", 64));
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^INTEGER$", 32));
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^INT$", 32));
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^SMALLINT$", 16));
    SQLTypeList.add(new SQLType(SQLType.Group.NUMBERS, "^REAL$", 64));
    // Precision
    SQLTypeList.add(new SQLType(SQLType.Group.PRECISION, "^DECIMAL(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.PRECISION, "^DEC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.PRECISION, "^FLOAT(\\s*\\(\\s*[1-9]\\d*\\s*\\))?$"));
    SQLTypeList.add(new SQLType(SQLType.Group.PRECISION, "^NUMERIC(\\s*\\(\\s*[1-9]\\d*\\s*(,\\s*\\d+\\s*)?\\))?$"));
  }
}
