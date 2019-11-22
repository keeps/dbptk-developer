/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;
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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
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
  private static final String A_M_591_1 = "A_M_5.9-1-1";
  private static final String M_591_2 = "M_5.9-1-2";
  private static final String A_M_591_2 = "A_M_5.9-1-2";
  private static final String M_591_3 = "M_5.9-1-3";
  private static final String A_M_591_3 = "A_M_5.9-1-3";
  private static final String M_591_4 = "M_5.9-1-4";
  private static final String A_M_591_8 = "A_M_5.9-1-8";

  private boolean additionalCheckError = false;
  private List<String> tableList = new ArrayList<>();
  private List<String> schemaList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataForeignKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    tableList.clear();
    schemaList.clear();
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_59);
    getValidationReporter().moduleValidatorHeader(M_59, MODULE_NAME);

    schemaList = getListOfSchemas();
    tableList = getListOfTables();

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:foreignKeys/ns:foreignKey", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read foreign key from SIARD file";
      setError(M_591, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_591, "Database has no foreign keys");
      observer.notifyValidationStep(MODULE_NAME, M_591, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_591, FOREIGN_KEYS_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:foreignKeys/ns:foreignKey");

    if (validateForeignKeyName(nodes)) {
      validationOk(MODULE_NAME, M_591_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_591_1, ValidationReporterStatus.ERROR);
    }

    // A_M_591_1
    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_591_1, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_591_1, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_591_1);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_591_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateForeignKeyReferencedSchema(nodes)) {
      validationOk(MODULE_NAME, M_591_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_591_2, ValidationReporterStatus.ERROR);
    }

    // A_M_591_2
    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_591_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_591_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_591_2);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_591_2, ValidationReporterStatus.ERROR);
      }
    }

    if (validateForeignKeyReferencedTable(nodes)) {
      validationOk(MODULE_NAME, M_591_3);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_591_3, ValidationReporterStatus.ERROR);
    }

    // A_M_591_3
    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_591_3, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_591_3, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_591_3);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_591_3, ValidationReporterStatus.ERROR);
      }
    }

    if (validateForeignKeyReference(nodes)) {
      validationOk(MODULE_NAME, M_591_4);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_591_4, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_591_8, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_591_8, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateForeignKeyDescription(nodes);
      validationOk(MODULE_NAME, A_M_591_8);
    }

    return reportValidations(MODULE_NAME);
  }

  private List<String> getListOfSchemas() {
    List<String> schemaList = new ArrayList<>();
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(is,
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
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(is,
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
  private boolean validateForeignKeyName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element foreignKey = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(foreignKey, Constants.TABLE);
      String schema = XMLUtils.getParentNameByTagName(foreignKey, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY,
        Integer.toString(i));

      String name = XMLUtils.getChildTextContext(foreignKey, Constants.NAME);

      if (validateXMLField(M_591_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(table + name) && !this.skipAdditionalChecks) {
          setError(A_M_591_1, String.format("Foreign key name %s must be unique per table (%s)", name, path));
          additionalCheckError = true;
          hasErrors = true;
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_591_1, String.format("Aborted because Foreign key name is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.9-1-2 The Foreign Key referenced schema is mandatory in SIARD 2.1
   * specification
   *
   * A_M_5.9-1-2 validation if this schema exists
   */
  private boolean validateForeignKeyReferencedSchema(NodeList nodes) {
    boolean hasErrors = false;
    additionalCheckError = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element foreignKey = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(foreignKey, Constants.TABLE);
      String schema = XMLUtils.getParentNameByTagName(foreignKey, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.FOREIGN_KEY,
        XMLUtils.getChildTextContext(foreignKey, Constants.NAME));

      String referencedSchema = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCED_SCHEMA);
      if (validateXMLField(M_591_2, referencedSchema, Constants.FOREIGN_KEY_REFERENCED_SCHEMA, true, false, path)) {
        if (!schemaList.contains(referencedSchema) && !this.skipAdditionalChecks) {
          setError(A_M_591_2,
            String.format("ReferencedSchema %s does not exist on database (%s)", referencedSchema, path));
          additionalCheckError = true;
          hasErrors = true;
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_591_2, String.format("Aborted because referencedSchema is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.9-1-3 The Foreign Key referenced table is mandatory in SIARD 2.1
   * specification
   * 
   * A_M_5.9-1-3 validation if this table exists
   */
  private boolean validateForeignKeyReferencedTable(NodeList nodes) {
    boolean hasErrors = false;
    additionalCheckError = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element foreignKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(foreignKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(foreignKey, Constants.TABLE), Constants.FOREIGN_KEY,
        XMLUtils.getChildTextContext(foreignKey, Constants.NAME));

      String referencedTable = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCED_TABLE);
      if (validateXMLField(M_591_3, referencedTable, Constants.FOREIGN_KEY_REFERENCED_TABLE, true, false, path)) {
        if (!tableList.contains(referencedTable) && !this.skipAdditionalChecks) {
          setError(A_M_591_3,
            String.format("ReferencedTable %s does not exist on database (%s)", referencedTable, path));
          additionalCheckError = true;
          hasErrors = true;
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_591_3, String.format("Aborted because referencedTable is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * M_5.9-1-4 The Foreign Key reference is mandatory in SIARD 2.1 specification
   */
  private boolean validateForeignKeyReference(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element foreignKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(foreignKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(foreignKey, Constants.TABLE), Constants.FOREIGN_KEY,
        XMLUtils.getChildTextContext(foreignKey, Constants.NAME));

      String reference = XMLUtils.getChildTextContext(foreignKey, Constants.FOREIGN_KEY_REFERENCE);
      if (!validateXMLField(M_591_4, reference, Constants.FOREIGN_KEY_REFERENCE, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.9-1-8 The foreign key description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateForeignKeyDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element foreignKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(foreignKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(foreignKey, Constants.TABLE), Constants.FOREIGN_KEY,
        XMLUtils.getChildTextContext(foreignKey, Constants.NAME));

      String description = XMLUtils.getChildTextContext(foreignKey, Constants.DESCRIPTION);

      validateXMLField(A_M_591_8, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
