package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
public class MetadataForeignKeyValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Foreign Key level metadata";
  private static final String M_59 = "5.9";
  private static final String M_591 = "M_5.9-1";
  private static final String M_591_1 = "M_5.9-1-1";
  private static final String M_591_2 = "M_5.9-1-2";
  private static final String M_591_3 = "M_5.9-1-3";
  private static final String M_591_8 = "M_5.9-1-8";

  private List<String> tableList = new ArrayList<>();
  private List<String> schemaList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataForeignKeyValidator newInstance() {
    return new MetadataForeignKeyValidator();
  }

  private MetadataForeignKeyValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_59, MODULE_NAME);

    schemaList = getListOfSchemas();
    tableList = getListOfTables();

    if (!readXMLMetadataForeignKeyLevel()) {
      reportValidations(M_591, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_591, MODULE_NAME) && reportValidations(M_591_1, MODULE_NAME)
      && reportValidations(M_591_2, MODULE_NAME) && reportValidations(M_591_3, MODULE_NAME)
      && reportValidations(M_591_8, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataForeignKeyLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = MetadataXMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

        NodeList foreignKeyNodes = tableElement.getElementsByTagName(Constants.FOREIGN_KEY);
        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKey = (Element) foreignKeyNodes.item(j);

          String name = MetadataXMLUtils.getChildTextContext(foreignKey, Constants.NAME);
          String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY, name);
          // * M_5.9-1 Foreign key name is mandatory.
          if (name == null || name.isEmpty()) {
            setError(M_591, "Foreign key name is required on table " + table);
            return false;
          }
          if (!validateForeignKeyName(table, name))
            break;

          String referencedSchema = MetadataXMLUtils.getChildTextContext(foreignKey,
            Constants.FOREIGN_KEY_REFERENCED_SCHEMA);
          // * M_5.9-1 Foreign key referencedSchema is mandatory.
          if (referencedSchema == null || referencedSchema.isEmpty()) {
            setError(M_591, String.format("ReferencedSchema is mandatory (%s)", path));
            return false;
          }
          if (!validateForeignKeyReferencedSchema(referencedSchema, path))
            break;

          String referencedTable = MetadataXMLUtils.getChildTextContext(foreignKey,
            Constants.FOREIGN_KEY_REFERENCED_TABLE);
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (referencedTable == null || referencedTable.isEmpty()) {
            setError(M_591, String.format("ReferencedTable is mandatory (%s)", path));
            return false;
          }
          if (!validateForeignKeyReferencedTable(referencedTable, name))
            break;

          String reference = MetadataXMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCE);
          // * M_5.9-1 Foreign key referencedTable is mandatory.
          if (reference == null || reference.isEmpty()) {
            setError(M_591, String.format("Reference is mandatory (%s)", path));
            return false;
          }

          String description = MetadataXMLUtils.getChildTextContext(foreignKey, Constants.DESCRIPTION);
          if (!validateForeignKeyDescription(description, path))
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
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

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
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tables = (Element) nodes.item(i);
        tableList.add(MetadataXMLUtils.getChildTextContext(tables, Constants.NAME));
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
      setError(M_591_1, String.format("Foreign key name %s of table %s must be unique", name, table));
      return false;
    }

    return true;
  }

  /**
   * M_5.9-1-2 The Schema in referencedSchema must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedSchema(String referencedSchema, String path) {
    if (!schemaList.contains(referencedSchema)) {
      setError(M_591_2, String.format("ReferencedSchema %s does not exist on database (%s)", referencedSchema, path));
      return false;
    }
    return true;
  }

  /**
   * M_5.9-1-3 The Table in referencedTable must exist. ERROR if not exist
   *
   * @return true if valid otherwise false
   */
  private boolean validateForeignKeyReferencedTable(String referencedTable, String path) {
    if (!tableList.contains(referencedTable)) {
      setError(M_591_3, String.format("ReferencedTable %s does not exist on database (%s)", referencedTable, path));
      return false;
    }

    return true;
  }

  /**
   * M_5.9-1-8 The foreign key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateForeignKeyDescription(String description, String path) {
    return validateXMLField(M_591_8, description, Constants.DESCRIPTION, false, true, path);
  }
}
