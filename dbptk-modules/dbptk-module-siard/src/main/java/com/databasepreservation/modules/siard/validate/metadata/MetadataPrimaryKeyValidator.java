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
public class MetadataPrimaryKeyValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Primary Key level metadata";
  private static final String M_58 = "5.8";
  private static final String M_581 = "M_5.8-1";
  private static final String M_581_1 = "M_5.8-1-1";
  private static final String M_581_2 = "M_5.8-1-2";
  private static final String M_581_3 = "M_5.8-1-3";
  private static final String BLANK = " ";

  private List<String> nameList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();
  private Map<String, Integer> primaryKeysCount = new HashMap<>();
  private Map<String, ArrayList<String>> tableColumnsList = new HashMap<>();
  private Map<String, ArrayList<String>> primaryKeyColumnList = new HashMap<>();

  public static MetadataPrimaryKeyValidator newInstance() {
    return new MetadataPrimaryKeyValidator();
  }

  private MetadataPrimaryKeyValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_58, MODULE_NAME);

    if (!reportValidations(readXMLMetadataPrimaryKeyLevel(), M_581, true)) {
      return false;
    }

    if (!reportValidations(validatePrimaryKeyName(), M_581_1, true)) {
      return false;
    }

    if (!reportValidations(validatePrimaryKeyColumn(), M_581_2, true)) {
      return false;
    }

    if (!reportValidations(validatePrimaryKeyDescription(), M_581_3, true)) {
      return false;
    }

    return true;
  }

  private boolean readXMLMetadataPrimaryKeyLevel() {

    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

        Element tableColumnsElement = MetadataXMLUtils.getChild(tableElement, "columns");
        if (tableColumnsElement == null) {
          return false;
        }
        NodeList tableColumns = tableColumnsElement.getElementsByTagName("column");

        ArrayList<String> tableColumnName = new ArrayList<>();
        for (int ci = 0; ci < tableColumns.getLength(); ci++) {
          tableColumnName.add(MetadataXMLUtils.getChildTextContext((Element) tableColumns.item(ci), "name"));
        }
        tableColumnsList.put(table, tableColumnName);

        NodeList primaryKeyNodes = tableElement.getElementsByTagName("primaryKey");
        primaryKeysCount.put(table, primaryKeyNodes.getLength());

        for (int j = 0; j < primaryKeyNodes.getLength(); j++) {
          Element primaryKey = (Element) primaryKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(primaryKey, "name");

          // * M_5.8-1 Primary key name is mandatory.
          if (name == null || name.isEmpty()) {
            hasErrors = "Primary key name is required on table " + table;
            return false;
          }

          nameList.add(name);
          NodeList columns = primaryKey.getElementsByTagName("column");

          ArrayList<String> columnList = new ArrayList<>();
          for (int k = 0; k < columns.getLength(); k++) {
            String column = columns.item(k).getTextContent();
            // * M_5.8-1 Primary key column is mandatory.
            if (column == null || column.isEmpty()) {
              hasErrors = "Primary key column is required on table " + table;
              return false;
            }
            columnList.add(column);
          }
          primaryKeyColumnList.put(table, columnList);
          descriptionList.add(MetadataXMLUtils.getChildTextContext(primaryKey, "description"));

        }

      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.8-1-1 The Primary Key name of table in SIARD file must be unique. ERROR
   * if not unique or is null. WARNING if contain any blanks
   *
   * @return true if valid otherwise false
   */
  private boolean validatePrimaryKeyName() {
    for (Map.Entry<String, Integer> entry : primaryKeysCount.entrySet()) {
      if (entry.getValue() > 1) {
        hasErrors = "Primary key must be unique on table " + entry.getKey();
        return false;
      }
    }

    for (String name : nameList) {
      if (name.contains(BLANK)) {
        hasWarnings.add("Primary key " + name + " contain blanks in name");
      }
    }

    return true;
  }

  /**
   * M_5.8-1-2 The Primary Key column in SIARD file must exist on table. ERROR if
   * not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validatePrimaryKeyColumn() {
    for (Map.Entry<String, ArrayList<String>> primaryKeyColumn : primaryKeyColumnList.entrySet()) {
      String tableName = primaryKeyColumn.getKey();
      List<String> columnReference = primaryKeyColumn.getValue();

      for (Map.Entry<String, ArrayList<String>> entry : tableColumnsList.entrySet()) {
        if (tableName.equals(entry.getKey())) {
          for (String column : columnReference) {
            if (!entry.getValue().contains(column)) {
              hasErrors = "Primary key column reference " + column + " not found on table " + tableName;
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * M_5.8-1-3 The primary key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validatePrimaryKeyDescription() {
    validateXMLFieldSizeList(descriptionList, "description");
    return true;
  }
}
