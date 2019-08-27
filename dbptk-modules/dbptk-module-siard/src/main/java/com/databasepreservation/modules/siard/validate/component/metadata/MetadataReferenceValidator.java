package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class MetadataReferenceValidator extends MetadataValidator {
  private final String MODULE_NAME;
  private static final String M_510 = "5.10";
  private static final String M_510_1 = "M_5.10-1";
  private static final String M_510_1_1 = "M_5.10-1-1";
  private static final String M_510_1_2 = "M_5.10-1-2";

  private static final String REFERENCE = "reference";
  private static final String REFERENCED_TABLE = "referencedTable";
  private static final String REFERENCED_COLUMN = "referenced";

  private List<String> tableList = new ArrayList<>();
  private Map<String, HashMap<String, String>> tableColumnsList = new HashMap<>();
  private Map<String, List<String>> primaryKeyList = new HashMap<>();
  private Map<String, List<String>> candidateKeyList = new HashMap<>();

  public MetadataReferenceValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_510_1, M_510_1_1, M_510_1_2);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_510);
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_510, MODULE_NAME);

    readXMLMetadataReferenceLevel();

    if (!readXMLMetadataReferenceLevel()) {
      reportValidations(M_510_1, MODULE_NAME);
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

  private boolean readXMLMetadataReferenceLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      tableColumnsList = getListColumnsByTables(nodes);
      primaryKeyList = getListKeysByTables(nodes, Constants.PRIMARY_KEY);
      candidateKeyList = getListKeysByTables(nodes, Constants.CANDIDATE_KEY);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);
        tableList.add(table);

        NodeList foreignKeyNodes = tableElement.getElementsByTagName(Constants.FOREIGN_KEY);
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKeyElement = (Element) foreignKeyNodes.item(j);
          String foreignKey = XMLUtils.getChildTextContext(foreignKeyElement, Constants.NAME);
          String referencedTable = XMLUtils.getChildTextContext(foreignKeyElement, REFERENCED_TABLE);

          NodeList referenceNodes = foreignKeyElement.getElementsByTagName(REFERENCE);
          for (int k = 0; k < referenceNodes.getLength(); k++) {
            Element reference = (Element) referenceNodes.item(k);
            String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY,
              foreignKey);

            String column = XMLUtils.getChildTextContext(reference, Constants.COLUMN);
            // * M_5.10-1 reference column is mandatory.
            if (column == null || column.isEmpty()) {
              setError(M_510_1, String.format("Column is required (%s)", path));
              return false;
            }
            if (!validateColumn(table, column))
              break;

            String referenced = XMLUtils.getChildTextContext(reference, REFERENCED_COLUMN);
            // * M_5.10-1 reference column is mandatory.
            if (referenced == null || referenced.isEmpty()) {
              setError(M_510_1, String.format("Referenced column is required (%s)", path));
              return false;
            }

            if (!validateReferencedColumn(table, referencedTable, column, referenced, foreignKey))
              break;
          }
        }

      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  private Map<String, HashMap<String, String>> getListColumnsByTables(NodeList tableNodes) {
    Map<String, HashMap<String, String>> columnsTables = new HashMap<>();

    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element tableElement = (Element) tableNodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);

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
      columnsTables.put(table, columnsNameList);
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
   * M_5.10-1-1 The column in reference must exist on table. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumn(String table, String column) {
    if (tableColumnsList.get(table).get(column) == null) {
      setError(M_510_1_1,
        String.format("referenced column name %s does not exist on referenced table %s", column, table));
      return false;
    }
    return true;
  }

  /**
   * M_5.10-1-2 The referenced column in reference must exist on table. ERROR if
   * not exist
   *
   * Additional check 1: Validation that fk and reference table pk have identical
   * data types
   *
   * Additional check 2: validation that all instances of foreign keys refer to
   * the primary key with existing record
   *
   * @return true if valid otherwise false
   */
  private boolean validateReferencedColumn(String foreignKeyTable, String referencedTable, String column,
    String referencedColumn, String foreignKey) {
    HashMap<String, String> referencedColumnTable = tableColumnsList.get(referencedTable);
    HashMap<String, String> foreignKeyColumnTable = tableColumnsList.get(foreignKeyTable);
    List<String> primaryKeyColumns = primaryKeyList.get(referencedTable);
    List<String> candidateKeyColumns = candidateKeyList.get(referencedTable);

    // M_5.10-1-2
    if (referencedColumnTable.get(referencedColumn) == null) {
      setError(M_510_1_2, String.format("referenced column name %s of table %s does not exist on referenced table %s",
        referencedColumn, foreignKeyTable, referencedTable));
      return false;
    }

    String foreignKeyType = foreignKeyColumnTable.get(column);
    if (primaryKeyColumns != null && primaryKeyColumns.contains(referencedColumn)) {
      // Additional check 1
      for (String primaryKey : primaryKeyColumns) {
        String primaryKeyType = referencedColumnTable.get(primaryKey);

        if (!primaryKeyType.equals(foreignKeyType)) {
          setError(M_510_1_2,
            String.format("Foreign Key %s.%s type %s does not match with type %s of Primary Key %s.%s", foreignKeyTable,
              foreignKey, foreignKeyType, primaryKeyType, referencedTable, primaryKey));
          return false;
        }
      }
    } else if (candidateKeyColumns != null && candidateKeyColumns.contains(referencedColumn)) {
      // Additional check 1
      for (String candidateKey : candidateKeyColumns) {
        String candidateKeyType = referencedColumnTable.get(candidateKey);

        if (!candidateKeyType.equals(foreignKeyType)) {
          setError(M_510_1_2,
            String.format("Foreign Key %s.%s type %s does not match with type %s of Candidate Key %s.%s",
              foreignKeyTable, foreignKey, foreignKeyType, candidateKeyType, referencedTable, candidateKey));
          return false;
        }
      }
    } else {
      // Additional check 2
      setError(M_510_1_2, String.format("Foreign Key %s.%s has no reference to any key on referenced table %s",
        foreignKeyTable, foreignKey, referencedTable));
      return false;
    }

    return true;
  }
}
