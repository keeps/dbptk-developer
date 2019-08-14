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

  private List<String> tableList = new ArrayList<>();
  private Map<String, HashMap<String, String>> referencedList = new HashMap<>();
  private Map<String, String> columnList = new HashMap<>();
  private Map<String, HashMap<String, String>> tableColumnsList = new HashMap<>();
  private Map<String, List<String>> primaryKeyList = new HashMap<>();

  public static MetadataReferenceValidator newInstance() {
    return new MetadataReferenceValidator();
  }

  private MetadataReferenceValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_510, MODULE_NAME);

    if (!reportValidations(readXMLMetadataReferenceLevel(), M_5101, true)) {
      return false;
    }

    if (!reportValidations(validateColumn(), M_5101_1, true)) {
      return false;
    }
    if (!reportValidations(validateReferencedColumn(), M_5101_2, true)) {
      return false;
    }

    return true;
  }

  private boolean readXMLMetadataReferenceLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      tableColumnsList = getListColumnsByTables(nodes);
      primaryKeyList = getListPrimaryKeysByTables(nodes);

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

            String column = MetadataXMLUtils.getChildTextContext(reference, "column");
            // * M_5.10-1 reference column is mandatory.
            if (column == null || column.isEmpty()) {
              hasErrors = "column is required on foreign key " + foreignKey;
              return false;
            }
            columnList.put(table, column);

            String referenced = MetadataXMLUtils.getChildTextContext(reference, "referenced");
            // * M_5.10-1 reference column is mandatory.
            if (referenced == null || referenced.isEmpty()) {
              hasErrors = "referenced column is required on foreign key " + foreignKey;
              return false;
            }
            HashMap<String, String> referencedMap = new HashMap<>();
            referencedMap.put("referencedTable", referencedTable);
            referencedMap.put("column", column);
            referencedMap.put("referencedColumn", referenced);
            referencedList.put(table, referencedMap);
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

  private Map<String, List<String>> getListPrimaryKeysByTables(NodeList tableNodes) {
    Map<String, List<String>> primaryKeysTables = new HashMap<>();

    for (int i = 0; i < tableNodes.getLength(); i++) {
      Element tableElement = (Element) tableNodes.item(i);
      String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

      Element primaryKeyElement = MetadataXMLUtils.getChild(tableElement, "primaryKey");
      if (primaryKeyElement == null) {
        continue;
      }

      NodeList columnNode = primaryKeyElement.getElementsByTagName("column");
      List<String> columnsNameList = new ArrayList<>();
      for (int j = 0; j < columnNode.getLength(); j++) {
        String name = columnNode.item(j).getTextContent();
        columnsNameList.add(name);
      }
      primaryKeysTables.put(table, columnsNameList);
    }

    return primaryKeysTables;
  }

  /**
   * M_5.10-1-1 The column in reference must exist on table. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumn() {
    for (Map.Entry<String, String> entry : columnList.entrySet()) {
      if (tableColumnsList.get(entry.getKey()).get(entry.getValue()) == null) {
        hasErrors = "column name " + entry.getValue() + " of Foreign key does not exist on table " + entry.getKey();
        return false;
      }
    }
    return true;
  }

  /**
   * M_5.10-1-2 The referenced column in reference must exist on table. ERROR if
   * not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateReferencedColumn() {
    for (Map.Entry<String, HashMap<String, String>> entry : referencedList.entrySet()) {
      String referencedTable = entry.getValue().get("referencedTable");
      String referencedColumn = entry.getValue().get("referencedColumn");
      String foreignKeyColumn = entry.getValue().get("column");
      String table = entry.getKey();

      HashMap<String, String> referencedColumnTable = tableColumnsList.get(referencedTable);
      HashMap<String, String> foreignKeyColumnTable = tableColumnsList.get(table);
      List<String> primaryKey = primaryKeyList.get(referencedTable);

      if (referencedColumnTable.get(referencedColumn) == null) {
        hasErrors = "reference column name " + referencedColumn + " of table " + table + " does not exist on table "
          + referencedTable;
        return false;
      }

      for (String primaryKeyColumns : primaryKey) {
        String primaryKeyColumnType = referencedColumnTable.get(primaryKeyColumns);
        String foreignKeyColumnType = foreignKeyColumnTable.get(foreignKeyColumn);

        if (!primaryKeyColumnType.equals(foreignKeyColumnType)) {
          hasErrors = "FK " + table + "." + foreignKeyColumn + " type " + foreignKeyColumnType
            + " does not match with type " + primaryKeyColumnType + " of PK " + referencedTable + "."
            + primaryKeyColumns;
          return false;
        }
      }

    }
    return true;
  }
}
