package com.databasepreservation.modules.siard.validate.metadata;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private List<String> nameList = new ArrayList<>();
  private List<String> referencedSchemaList = new ArrayList<>();
  private List<String> referencedTableList = new ArrayList<>();
  private List<String> referenceList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();
  private List<String> tableList = new ArrayList<>();

  public static MetadataForeignKeyValidator newInstance() {
    return new MetadataForeignKeyValidator();
  }

  private MetadataForeignKeyValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_59, MODULE_NAME);

    if (!reportValidations(readXMLMetadataForeignKeyLevel(), M_591, true)) {
      return false;
    }

    if (!reportValidations(validateForeignKeyName(), M_591_1, true)) {
      return false;
    }

    if (!reportValidations(validateForeignKeyReferencedSchema(), M_591_2, true)) {
      return false;
    }

    if (!reportValidations(validateForeignKeyReferencedTable(), M_591_3, true)) {
      return false;
    }

    if (!reportValidations(validateForeignKeyDescription(), M_591_8, true)) {
      return false;
    }
    return true;
  }

  private boolean readXMLMetadataForeignKeyLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        tableList.add(table);

        NodeList foreignKeyNodes = tableElement.getElementsByTagName("foreignKey");
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKey = (Element) foreignKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(foreignKey, "name");
          // * M_5.9-1 Foreign key name is mandatory.
          if (name == null || name.isEmpty()) {
            hasErrors = "Foreign key name is required on table " + table;
            return false;
          }
          nameList.add(name);

          String referencedSchema = MetadataXMLUtils.getChildTextContext(foreignKey, "referencedSchema");
          // * M_5.9-1 Foreign key referencedSchema is mandatory.
          if (referencedSchema == null || referencedSchema.isEmpty()) {
            hasErrors = "Foreign key referencedSchema is required on table " + table;
            return false;
          }
          referencedSchemaList.add(referencedSchema);

          String referencedTable = MetadataXMLUtils.getChildTextContext(foreignKey, "referencedTable");
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (referencedTable == null || referencedTable.isEmpty()) {
            hasErrors = "Foreign key referencedTable is required on table " + table;
            return false;
          }
          referencedTableList.add(referencedTable);

          String reference = MetadataXMLUtils.getChildTextContext(foreignKey, "reference");
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (reference == null || reference.isEmpty()) {
            hasErrors = "Foreign key reference is required on table " + table;
            return false;
          }
          referenceList.add(reference);

          descriptionList.add(MetadataXMLUtils.getChildTextContext(foreignKey, "description"));
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

  /**
   * M_5.9-1-1 The Foreign Key name in SIARD file must be unique. ERROR if not
   * unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyName() {
    Set<String> checkDuplicates = new HashSet<>();
    for (String name : nameList) {
      if (checkDuplicates.contains(name)) {
        hasErrors = "Foreign key name " + name + " must be unique";
        return false;
      }
      checkDuplicates.add(name);
    }

    return true;
  }

  /**
   * M_5.9-1-2 The Schema in referencedSchema must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedSchema() {
    List<String> schemaList = getListOfSchemas();
    for (String schema : referencedSchemaList) {
      if (!schemaList.contains(schema)) {
        hasErrors = "Foreign key referencedSchema " + schema + " does not exist on database";
        return false;
      }
    }

    return true;
  }

  /**
   * M_5.9-1-3 The Table in referencedTable must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedTable() {
    for (String table : referencedTableList) {
      if (!tableList.contains(table)) {
        hasErrors = "Foreign key referencedTable " + table + " does not exist on database";
        return false;
      }
    }

    return true;
  }

  /**
   * M_5.9-1-8 The foreign key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateForeignKeyDescription() {
    validateXMLFieldSizeList(descriptionList, "description");
    return true;
  }
}
