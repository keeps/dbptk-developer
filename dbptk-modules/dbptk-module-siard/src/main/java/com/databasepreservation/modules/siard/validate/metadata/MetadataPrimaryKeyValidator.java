package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private static final String SCHEMA = "schema";
  private static final String TABLE = "table";
  private static final String PRIMARY_KEY = "primaryKey";
  private static final String PRIMARY_KEY_NAME = "name";
  private static final String PRIMARY_KEY_COLUMN = "column";
  private static final String PRIMARY_KEY_DESCRIPTION = "description";

  private Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();

  public static MetadataPrimaryKeyValidator newInstance() {
    return new MetadataPrimaryKeyValidator();
  }

  private MetadataPrimaryKeyValidator() {
    error.clear();
    warnings.clear();
    warnings.put(PRIMARY_KEY, new ArrayList<String>());
    warnings.put(PRIMARY_KEY_NAME, new ArrayList<String>());
    warnings.put(PRIMARY_KEY_COLUMN, new ArrayList<String>());
    warnings.put(PRIMARY_KEY_DESCRIPTION, new ArrayList<String>());
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_58, MODULE_NAME);

    readXMLMetadataPrimaryKeyLevel();

    return reportValidations(M_581, PRIMARY_KEY) && reportValidations(M_581_1, PRIMARY_KEY_NAME)
      && reportValidations(M_581_2, PRIMARY_KEY_COLUMN) && reportValidations(M_581_3, PRIMARY_KEY_DESCRIPTION);
  }

  private boolean readXMLMetadataPrimaryKeyLevel() {

    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        String tableFolder = MetadataXMLUtils.getChildTextContext(tableElement, "folder");
        String schema = MetadataXMLUtils.getChildTextContext((Element) tableElement.getParentNode().getParentNode(),
          "name");
        String schemaFolder = MetadataXMLUtils
          .getChildTextContext((Element) tableElement.getParentNode().getParentNode(), "folder");

        Element tableColumnsElement = MetadataXMLUtils.getChild(tableElement, "columns");
        if (tableColumnsElement == null) {
          return false;
        }
        NodeList tableColumns = tableColumnsElement.getElementsByTagName("column");

        LinkedList<String> tableColumnName = new LinkedList<>();
        for (int ci = 0; ci < tableColumns.getLength(); ci++) {
          tableColumnName.add(MetadataXMLUtils.getChildTextContext((Element) tableColumns.item(ci), "name"));
        }
        tableColumnsList.put(table, tableColumnName);

        NodeList primaryKeyNodes = tableElement.getElementsByTagName("primaryKey");
        // primaryKeysCount.put(table, primaryKeyNodes.getLength());

        for (int j = 0; j < primaryKeyNodes.getLength(); j++) {
          Element primaryKey = (Element) primaryKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(primaryKey, PRIMARY_KEY_NAME);

          // * M_5.8-1 Primary key name is mandatory.
          if (name == null || name.isEmpty()) {
            error.put(PRIMARY_KEY, "Primary key name is required on table " + table);
            return false;
          }

          // nameList.add(name);
          NodeList columns = primaryKey.getElementsByTagName(PRIMARY_KEY_COLUMN);

          ArrayList<String> columnList = new ArrayList<>();
          for (int k = 0; k < columns.getLength(); k++) {
            String column = columns.item(k).getTextContent();
            // * M_5.8-1 Primary key column is mandatory.
            if (column == null || column.isEmpty()) {
              error.put(PRIMARY_KEY, "Primary key column is required on table " + table);
              return false;
            }
            columnList.add(column);
          }

          if (!validatePrimaryKeyName(schemaFolder, tableFolder, table, name, columnList))
            break;

          if (!validatePrimaryKeyColumn(schema, table, name, columnList))
            break;

          String description = MetadataXMLUtils.getChildTextContext(primaryKey, PRIMARY_KEY_DESCRIPTION);
          if (!validatePrimaryKeyDescription(schema, table, name, description))
            break;

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
  private boolean validatePrimaryKeyName(String schemaFolder, String tableFolder, String table, String name,
    ArrayList<String> columnList) {

    List<String> columns = new ArrayList<>();
    for (String column : columnList) {
      if (tableColumnsList.get(table).indexOf(column) >= 0) {
        int columnIndex = tableColumnsList.get(table).indexOf(column) + 1;
        columns.add("c" + columnIndex);
      } else {
        error.put(PRIMARY_KEY_NAME, String.format("Column %s does not exist on table %s", column, table));
        return false;
      }
    }

    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = MetadataXMLUtils.createPath(MetadataXMLUtils.SIARD_CONTENT, schemaFolder, tableFolder,
        tableFolder + MetadataXMLUtils.XML_EXTENSION);
      String xpathExpression = "/ns:table/ns:row";
      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET,
        MetadataXMLUtils.TABLE);

      Set<String> unique = new HashSet<>();

      for (int i = 0; i < nodes.getLength(); i++) {
        Element row = (Element) nodes.item(i);

        StringBuilder primaryColumn = new StringBuilder();
        for (int j = 0; j < columns.size(); j++) {
          String columnText = MetadataXMLUtils.getChildTextContext(row, columns.get(j));
          if (j > 0 && j < columns.size()) {
            primaryColumn.append(".");
          }
          primaryColumn.append(columnText);
        }

        if (!unique.add(primaryColumn.toString())) {
          error.put(PRIMARY_KEY_NAME,
            String.format("Found duplicates primary keys '%s' with value %s on %s.%s column %s", name,
              primaryColumn.toString(), schemaFolder, tableFolder, columns.toString()));
          return false;
        }
        ;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    if (name.contains(BLANK)) {
      warnings.get(PRIMARY_KEY_NAME).add("Primary key " + name + " contain blanks in name");
    }
    return true;
  }

  /**
   * M_5.8-1-2 The Primary Key column in SIARD file must exist on table. ERROR if
   * not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validatePrimaryKeyColumn(String schema, String table, String name, List<String> columnList) {

    for (String column : columnList) {
      if (!tableColumnsList.get(table).contains(column)) {
        error.put(PRIMARY_KEY_COLUMN,
          String.format("Primary key column reference %s not found on %s.%s" + column, schema, table));
        return false;
      }
    }
    return true;
  }

  /**
   * M_5.8-1-3 The primary key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validatePrimaryKeyDescription(String schema, String table, String name, String description) {
    return validateXMLField(description, PRIMARY_KEY_DESCRIPTION, false, true, SCHEMA, schema, TABLE, table,
      PRIMARY_KEY, name);
  }
}
