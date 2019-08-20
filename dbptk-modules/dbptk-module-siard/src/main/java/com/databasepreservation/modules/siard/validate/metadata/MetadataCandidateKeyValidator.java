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

import com.databasepreservation.model.modules.validate.ValidatorModule;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataCandidateKeyValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Candidate Key level metadata";
  private static final String M_511 = "5.11";
  private static final String M_511_1 = "M_5.11-1";
  private static final String M_511_1_1 = "M_5.11-1-1";
  private static final String M_511_1_2 = "M_5.11-1-2";
  private static final String M_511_1_3 = "M_5.11-1-3";

  private static final String SCHEMA = "schema";
  private static final String TABLE = "table";
  private static final String CANDIDATE_KEY = "candidateKey";
  private static final String CANDIDATE_KEY_NAME = "name";
  private static final String CANDIDATE_KEY_COLUMN = "column";
  private static final String CANDIDATE_KEY_DESCRIPTION = "description";

  private Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();

  public static ValidatorModule newInstance() {
    return new MetadataCandidateKeyValidator();
  }

  private MetadataCandidateKeyValidator() {
    error.clear();
    warnings.clear();
    warnings.put(CANDIDATE_KEY, new ArrayList<String>());
    warnings.put(CANDIDATE_KEY_NAME, new ArrayList<String>());
    warnings.put(CANDIDATE_KEY_COLUMN, new ArrayList<String>());
    warnings.put(CANDIDATE_KEY_DESCRIPTION, new ArrayList<String>());
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_511, MODULE_NAME);

    readXMLMetadataCandidateKeyLevel();

    return reportValidations(M_511_1, CANDIDATE_KEY) && reportValidations(M_511_1_1, CANDIDATE_KEY_NAME)
      && reportValidations(M_511_1_2, CANDIDATE_KEY_COLUMN) && reportValidations(M_511_1_3, CANDIDATE_KEY_DESCRIPTION);
  }

  private boolean readXMLMetadataCandidateKeyLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {

      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";
      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        String schema = MetadataXMLUtils.getChildTextContext((Element) tableElement.getParentNode().getParentNode(),
          "name");
        String tableFolder = MetadataXMLUtils.getChildTextContext(tableElement, "folder");
        String schemaFolder = MetadataXMLUtils
          .getChildTextContext((Element) tableElement.getParentNode().getParentNode(), "folder");

        Element tableColumnsElement = MetadataXMLUtils.getChild(tableElement, "columns");
        if (tableColumnsElement == null) {
          return false;
        }
        NodeList tableColumns = tableColumnsElement.getElementsByTagName("column");
        LinkedList<String> tableColumnName = new LinkedList<>();

        for (int j = 0; j < tableColumns.getLength(); j++) {
          tableColumnName.add(MetadataXMLUtils.getChildTextContext((Element) tableColumns.item(j), "name"));
        }
        tableColumnsList.put(table, tableColumnName);

        NodeList candidateKeyNodes = tableElement.getElementsByTagName("candidateKey");
        // candidateKeysCount.put(table, candidateKeyNodes.getLength());

        for (int j = 0; j < candidateKeyNodes.getLength(); j++) {
          Element candidateKey = (Element) candidateKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(candidateKey, CANDIDATE_KEY_NAME);

          // * M_5.8-1 Candidate key name is mandatory.
          if (name == null || name.isEmpty()) {
            error.put(CANDIDATE_KEY, "Candidate key name is required on table " + table);
            return false;
          }

          // nameList.add(name);
          NodeList columns = candidateKey.getElementsByTagName(CANDIDATE_KEY_COLUMN);

          ArrayList<String> columnList = new ArrayList<>();
          for (int k = 0; k < columns.getLength(); k++) {
            String column = columns.item(k).getTextContent();
            // * M_5.8-1 Candidate key column is mandatory.
            if (column == null || column.isEmpty()) {
              error.put(CANDIDATE_KEY, "Candidate key column is required on table " + table);
              return false;
            }
            columnList.add(column);
          }

          if (!validateCandidateKeyName(schemaFolder, tableFolder, table, name, columnList))
            break;

          if (!validateCandidateKeyColumn(schema, table, name, columnList))
            break;

          String description = MetadataXMLUtils.getChildTextContext(candidateKey, CANDIDATE_KEY_DESCRIPTION);
          if (!validateCandidateKeyDescription(schema, table, name, description))
            break;

        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.11-1-1 The Candidate Key name of table in SIARD file must be unique.
   * ERROR if not unique or is null.
   *
   * @return true if valid otherwise false
   */
  private boolean validateCandidateKeyName(String schemaFolder, String tableFolder, String table, String name,
    ArrayList<String> columnList) {
    List<String> columns = new ArrayList<>();
    for (String column : columnList) {
      if (tableColumnsList.get(table).indexOf(column) >= 0) {
        int columnIndex = tableColumnsList.get(table).indexOf(column) + 1;
        columns.add("c" + columnIndex);
      } else {
        error.put(CANDIDATE_KEY_NAME, String.format("Column %s does not exist on table %s", column, table));
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

        StringBuilder candidateColumn = new StringBuilder();
        for (int j = 0; j < columns.size(); j++) {
          String columnText = MetadataXMLUtils.getChildTextContext(row, columns.get(j));
          if (j > 0 && j < columns.size()) {
            candidateColumn.append(".");
          }
          candidateColumn.append(columnText);
        }

        if (!unique.add(candidateColumn.toString())) {
          error.put(CANDIDATE_KEY_NAME,
            String.format("Found duplicates candidate keys '%s' with value %s on %s.%s column %s", name,
              candidateColumn.toString(), schemaFolder, tableFolder, columns.toString()));
          return false;
        }
        ;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

    /**
     * M_5.11-1-2 The Candidate Key column in SIARD file must exist on table. ERROR if
     * not exist
     *
     * @return true if valid otherwise false
     */
    private boolean validateCandidateKeyColumn(String schema, String table, String name, ArrayList<String> columnList) {
        for (String column : columnList) {
            if (!tableColumnsList.get(table).contains(column)) {
                error.put(CANDIDATE_KEY_COLUMN,
                        String.format("Candidate key column reference %s not found on %s.%s" + column, schema, table));
                return false;
            }
        }
        return true;
    }

    /**
     * M_5.11-1-3 The Candidate key description in SIARD file must not be less than 3
     * characters. WARNING if it is less than 3 characters
     */
    private boolean validateCandidateKeyDescription(String schema, String table, String name, String description) {
        return validateXMLField(description, CANDIDATE_KEY_DESCRIPTION, false, true, SCHEMA, schema, TABLE, table,
                CANDIDATE_KEY, name);
    }
}
