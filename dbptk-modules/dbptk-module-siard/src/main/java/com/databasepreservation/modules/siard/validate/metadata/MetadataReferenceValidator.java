package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataReferenceValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Reference level metadata";
  private static final String M_510 = "5.10";
  private static final String M_5101 = "M_5.10-1";
  private static final String M_5101_1 = "M_5.10-1-1";
  private static final String M_5101_2 = "M_5.10-1-2";

  private static final String REFERENCE = "reference";
  private static final String REFERENCE_COLUMN = "column";
  private static final String REFERENCE_COLUMN_REFERENCED = "referenced";

  private List<String> tableList = new ArrayList<>();
  private Map<String, HashMap<String, String>> tableColumnsList = new HashMap<>();
  private Map<String, List<String>> primaryKeyList = new HashMap<>();
  private Map<String, List<String>> candidateKeyList = new HashMap<>();

  public static MetadataReferenceValidator newInstance() {
    return new MetadataReferenceValidator();
  }

  private MetadataReferenceValidator() {
    error.clear();
    warnings.clear();
    warnings.put(REFERENCE, new ArrayList<String>());
    warnings.put(REFERENCE_COLUMN, new ArrayList<String>());
    warnings.put(REFERENCE_COLUMN_REFERENCED, new ArrayList<String>());
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_510, MODULE_NAME);

    readXMLMetadataReferenceLevel();

    return reportValidations(M_5101, REFERENCE) && reportValidations(M_5101_1, REFERENCE_COLUMN)
      && reportValidations(M_5101_2, REFERENCE_COLUMN_REFERENCED);
  }

  private boolean readXMLMetadataReferenceLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      tableColumnsList = getListColumnsByTables(nodes);
      primaryKeyList = getListKeysByTables(nodes, "primaryKey");
      candidateKeyList = getListKeysByTables(nodes, "candidateKey");

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        tableList.add(table);

        NodeList foreignKeyNodes = tableElement.getElementsByTagName("foreignKey");
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKeyElement = (Element) foreignKeyNodes.item(j);
          String foreignKey = MetadataXMLUtils.getChildTextContext(foreignKeyElement, "name");
          String referencedTable = MetadataXMLUtils.getChildTextContext(foreignKeyElement, "referencedTable");

          NodeList referenceNodes = foreignKeyElement.getElementsByTagName("reference");
          for (int k = 0; k < referenceNodes.getLength(); k++) {
            Element reference = (Element) referenceNodes.item(k);

            String column = MetadataXMLUtils.getChildTextContext(reference, REFERENCE_COLUMN);
            // * M_5.10-1 reference column is mandatory.
            if (column == null || column.isEmpty()) {
              error.put(REFERENCE, "column is required on foreign key " + foreignKey);
              return false;
            }
            if (!validateColumn(table, column))
              break;

            String referenced = MetadataXMLUtils.getChildTextContext(reference, REFERENCE_COLUMN_REFERENCED);
            // * M_5.10-1 reference column is mandatory.
            if (referenced == null || referenced.isEmpty()) {
              error.put(REFERENCE, "referenced column is required on foreign key " + foreignKey);
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
      String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

      Element tableColumnsElement = MetadataXMLUtils.getChild(tableElement, "columns");
      if (tableColumnsElement == null) {
        break;
      }

      NodeList columnNode = tableColumnsElement.getElementsByTagName("column");
      HashMap<String, String> columnsNameList = new HashMap<>();
      for (int j = 0; j < columnNode.getLength(); j++) {
        Element columnElement = (Element) columnNode.item(j);
        String name = MetadataXMLUtils.getChildTextContext(columnElement, "name");
        String type = MetadataXMLUtils.getChildTextContext(columnElement, "type");
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
      String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

      NodeList keyNodes = tableElement.getElementsByTagName(key);
      if (keyNodes == null) {
        continue;
      }

      List<String> columnsNameList = new ArrayList<>();
      for (int j = 0; j < keyNodes.getLength(); j++) {
        Element keyElement = (Element) keyNodes.item(j);
        NodeList columnNode = keyElement.getElementsByTagName("column");
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
      error.put(REFERENCE_COLUMN,
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
      error.put(REFERENCE_COLUMN_REFERENCED,
        String.format("referenced column name %s of table %s does not exist on referenced table %s", referencedColumn,
          foreignKeyTable, referencedTable));
      return false;
    }

    String foreignKeyType = foreignKeyColumnTable.get(column);
    if (primaryKeyColumns != null && primaryKeyColumns.contains(referencedColumn)) {
      // Additional check 1
      for (String primaryKey : primaryKeyColumns) {
        String primaryKeyType = referencedColumnTable.get(primaryKey);

        if (!primaryKeyType.equals(foreignKeyType)) {
          error.put(REFERENCE_COLUMN_REFERENCED,
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
          error.put(REFERENCE_COLUMN_REFERENCED,
            String.format("Foreign Key %s.%s type %s does not match with type %s of Candidate Key %s.%s",
              foreignKeyTable, foreignKey, foreignKeyType, candidateKeyType, referencedTable, candidateKey));
          return false;
        }
      }
    } else {
      // Additional check 2
      error.put(REFERENCE_COLUMN_REFERENCED,
        String.format("Foreign Key %s.%s has no reference to any key on referenced table %s", foreignKeyTable,
          foreignKey, referencedTable));
      return false;
    }

    return true;
  }
}
