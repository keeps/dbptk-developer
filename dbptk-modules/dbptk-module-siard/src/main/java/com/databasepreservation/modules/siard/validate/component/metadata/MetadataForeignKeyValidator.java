package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataForeignKeyValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataForeignKeyValidator.class);
  private final String MODULE_NAME;
  private static final String M_59 = "5.9";
  private static final String M_591 = "M_5.9-1";
  private static final String M_591_1 = "M_5.9-1-1";
  private static final String M_591_2 = "M_5.9-1-2";
  private static final String A_M_591_2 = "A_M_5.9-1-2";
  private static final String M_591_3 = "M_5.9-1-3";
  private static final String A_M_591_3 = "A_M_5.9-1-3";
  private static final String M_591_4 = "M_5.9-1-4";
  private static final String A_M_591_8 = "A_M_5.9-1-8";

  private List<Element> foreignKeyList = new ArrayList<>();
  private List<String> tableList = new ArrayList<>();
  private List<String> schemaList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataForeignKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_591, M_591_1, M_591_2, A_M_591_2, M_591_3, A_M_591_3, M_591_4, A_M_591_8);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_59);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_59, MODULE_NAME);

    schemaList = getListOfSchemas();
    tableList = getListOfTables();

    validateMandatoryXSDFields(M_591, FOREIGN_KEYS_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:foreignKeys/ns:foreignKey");

    if (!readXMLMetadataForeignKeyLevel()) {
      reportValidations(M_591, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (foreignKeyList.isEmpty()) {
      getValidationReporter().skipValidation(M_591, "Database has no foreign keys");
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataForeignKeyLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

        NodeList foreignKeyNodes = tableElement.getElementsByTagName(Constants.FOREIGN_KEY);
        if (foreignKeyNodes == null) {
          // next table
          continue;
        }

        for (int j = 0; j < foreignKeyNodes.getLength(); j++) {
          Element foreignKey = (Element) foreignKeyNodes.item(j);
          foreignKeyList.add(foreignKey);
          String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY,
            Integer.toString(j));

          String name = XMLUtils.getChildTextContext(foreignKey, Constants.NAME);
          validateForeignKeyName(name, path);

          path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY, name);

          String referencedSchema = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCED_SCHEMA);
          validateForeignKeyReferencedSchema(referencedSchema, path);

          String referencedTable = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCED_TABLE);
          validateForeignKeyReferencedTable(referencedTable, name);

          String reference = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCE);
          validateForeignKeyReference(reference, path);

          String description = XMLUtils.getChildTextContext(foreignKey, Constants.DESCRIPTION);
          validateForeignKeyDescription(description, path);
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read foreign key from SIARD file";
      setError(M_591, errorMessage);
      LOGGER.debug(errorMessage, e);
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
        schemaList.add(XMLUtils.getChildTextContext(schema, "name"));
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read schemas from SIARD file";
      LOGGER.debug(errorMessage, e);
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
        tableList.add(XMLUtils.getChildTextContext(tables, Constants.NAME));
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read tables from SIARD file";
      LOGGER.debug(errorMessage, e);
    }
    return tableList;
  }

  /**
   * M_5.9-1-1 The Foreign Key name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.9-1-1 The Foreign Key name in SIARD file must be unique. ERROR if not
   * unique
   */
  private void validateForeignKeyName(String name, String path) {
    validateXMLField(M_591_1, name, Constants.NAME, true, false, path);
    if (!checkDuplicates.add(name)) {
      setError(M_591_1, String.format("Foreign key name %s must be unique (%s)", name, path));
    }
  }

  /**
   * M_5.9-1-2 The Foreign Key referenced schema is mandatory in SIARD 2.1
   * specification
   *
   * A_M_5.9-1-2 validation if this schema exists
   */
  private void validateForeignKeyReferencedSchema(String referencedSchema, String path) {
    if(validateXMLField(M_591_2, referencedSchema, Constants.FOREIGN_KEY_REFERENCED_SCHEMA, true, false, path)){
      if (!schemaList.contains(referencedSchema)) {
        setError(A_M_591_2, String.format("ReferencedSchema %s does not exist on database (%s)", referencedSchema, path));
      }
      return;
    }
    setError(A_M_591_2, String.format("Aborted because referencedSchema is mandatory (%s)", path));
  }

  /**
   * M_5.9-1-3 The Foreign Key referenced table is mandatory in SIARD 2.1
   * specification
   * 
   * A_M_5.9-1-3 validation if this table exists
   */
  private void validateForeignKeyReferencedTable(String referencedTable, String path) {
    if(validateXMLField(M_591_3, referencedTable, Constants.FOREIGN_KEY_REFERENCED_TABLE, true, false, path)){
      if (!tableList.contains(referencedTable)) {
        setError(A_M_591_3, String.format("ReferencedTable %s does not exist on database (%s)", referencedTable, path));
      }
      return;
    }
    setError(A_M_591_3, String.format("Aborted because referencedTable is mandatory (%s)", path));
  }

  /**
   * M_5.9-1-4 The Foreign Key reference is mandatory in SIARD 2.1 specification
   */
  private void validateForeignKeyReference(String reference, String path) {
    validateXMLField(M_591_4, reference, Constants.FOREIGN_KEY_REFERENCE, true, false, path);
  }

  /**
   * A_M_5.9-1-8 The foreign key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateForeignKeyDescription(String description, String path) {
    validateXMLField(A_M_591_8, description, Constants.DESCRIPTION, false, true, path);
  }
}
