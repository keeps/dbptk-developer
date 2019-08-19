package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
public class MetadataForeignKeyValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Foreign Key level metadata";
  private static final String M_59 = "5.9";
  private static final String M_591 = "M_5.9-1";
  private static final String M_591_1 = "M_5.9-1-1";
  private static final String M_591_2 = "M_5.9-1-2";
  private static final String M_591_3 = "M_5.9-1-3";
  private static final String M_591_8 = "M_5.9-1-8";

  private static final String TABLE = "table";
  private static final String FOREIGN_KEY = "foreignKey";
  private static final String FOREIGN_KEY_NAME = "name";
  private static final String FOREIGN_KEY_REFERENCED_SCHEMA = "referencedSchema";
  private static final String FOREIGN_KEY_REFERENCED_TABLE = "referencedTable";
  private static final String FOREIGN_KEY_REFERENCE = "reference";
  private static final String FOREIGN_KEY_DESCRIPTION = "description";

  private List<String> tableList = new ArrayList<>();
  private List<String> schemaList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataForeignKeyValidator newInstance() {
    return new MetadataForeignKeyValidator();
  }

  private MetadataForeignKeyValidator() {
    error.clear();
    warnings.clear();
    warnings.put(FOREIGN_KEY, new ArrayList<String>());
    warnings.put(FOREIGN_KEY_NAME, new ArrayList<String>());
    warnings.put(FOREIGN_KEY_REFERENCED_SCHEMA, new ArrayList<String>());
    warnings.put(FOREIGN_KEY_REFERENCED_TABLE, new ArrayList<String>());
    warnings.put(FOREIGN_KEY_DESCRIPTION, new ArrayList<String>());
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_59, MODULE_NAME);

    schemaList = getListOfSchemas();
    tableList = getListOfTables();
    readXMLMetadataForeignKeyLevel();

    return reportValidations(M_591, FOREIGN_KEY) && reportValidations(M_591_1, FOREIGN_KEY_NAME)
      && reportValidations(M_591_2, FOREIGN_KEY_REFERENCED_SCHEMA)
      && reportValidations(M_591_3, FOREIGN_KEY_REFERENCED_TABLE)
      && reportValidations(M_591_8, FOREIGN_KEY_DESCRIPTION);
  }

  private boolean readXMLMetadataForeignKeyLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

        NodeList foreignKeyNodes = tableElement.getElementsByTagName(FOREIGN_KEY);
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKey = (Element) foreignKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(foreignKey, FOREIGN_KEY_NAME);
          // * M_5.9-1 Foreign key name is mandatory.
          if (name == null || name.isEmpty()) {
            error.put(FOREIGN_KEY, "Foreign key name is required on table " + table);
            return false;
          }
          if (!validateForeignKeyName(table, name))
            break;

          String referencedSchema = MetadataXMLUtils.getChildTextContext(foreignKey, FOREIGN_KEY_REFERENCED_SCHEMA);
          // * M_5.9-1 Foreign key referencedSchema is mandatory.
          if (referencedSchema == null || referencedSchema.isEmpty()) {
            error.put(FOREIGN_KEY, "Foreign key referencedSchema is required on table " + table);
            return false;
          }
          if (!validateForeignKeyReferencedSchema(referencedSchema, name))
            break;

          String referencedTable = MetadataXMLUtils.getChildTextContext(foreignKey, FOREIGN_KEY_REFERENCED_TABLE);
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (referencedTable == null || referencedTable.isEmpty()) {
            error.put(FOREIGN_KEY, "Foreign key referencedTable is required on table " + table);
            return false;
          }
          if (!validateForeignKeyReferencedTable(referencedTable, name))
            break;

          String reference = MetadataXMLUtils.getChildTextContext(foreignKey, FOREIGN_KEY_REFERENCE);
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (reference == null || reference.isEmpty()) {
            error.put(FOREIGN_KEY, "Foreign key reference is required on table " + table);
            return false;
          }

          String description = MetadataXMLUtils.getChildTextContext(foreignKey, FOREIGN_KEY_DESCRIPTION);
          if (!validateForeignKeyDescription(description, table, name))
            break;
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  private List<String> getListOfSchemas() {
    List<String> schemaList = new ArrayList<>();
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);
        schemaList.add(MetadataXMLUtils.getChildTextContext(schema, "name"));
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      e.printStackTrace();
    }
    return schemaList;
  }

  private List<String> getListOfTables() {
    List<String> tableList = new ArrayList<>();
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tables = (Element) nodes.item(i);
        tableList.add(MetadataXMLUtils.getChildTextContext(tables, "name"));
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      e.printStackTrace();
    }
    return tableList;
  }

  /**
   * M_5.9-1-1 The Foreign Key name in SIARD file must be unique. ERROR if not
   * unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyName(String table, String name) {
    if (!checkDuplicates.add(name)) {
      error.put(FOREIGN_KEY_NAME, String.format("Foreign key name %s of table %s must be unique", name, table));
      return false;
    }

    return true;
  }

  /**
   * M_5.9-1-2 The Schema in referencedSchema must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedSchema(String referencedSchema, String name) {
    if (!schemaList.contains(referencedSchema)) {
      error.put(FOREIGN_KEY_REFERENCED_SCHEMA,
        String.format("Foreign key %s referencedSchema %s of does not exist on database", name, referencedSchema));
      return false;
    }
    return true;
  }

  /**
   * M_5.9-1-3 The Table in referencedTable must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedTable(String referencedTable, String name) {
    if (!tableList.contains(referencedTable)) {
      error.put(FOREIGN_KEY_REFERENCED_TABLE,
        String.format("Foreign key %s referencedTable %s of does not exist on database", name, referencedTable));
      return false;
    }

    return true;
  }

  /**
   * M_5.9-1-8 The foreign key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateForeignKeyDescription(String description, String table, String name) {
    return validateXMLField(description, FOREIGN_KEY_DESCRIPTION, false, true, TABLE, table, FOREIGN_KEY, name);
  }
}
