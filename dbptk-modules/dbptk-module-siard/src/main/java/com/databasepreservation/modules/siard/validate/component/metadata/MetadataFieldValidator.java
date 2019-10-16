/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class MetadataFieldValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataFieldValidator.class);
  private final String MODULE_NAME;
  private static final String M_57 = "5.7";
  private static final String M_571 = "M_5.7-1";
  private static final String M_571_1 = "M_5.7-1-1";
  private static final String A_M_571_2 = "A_M_5.7-1-2";
  private static final String A_M_571_5 = "A_M_5.7-1-5";
  private static final String ARRAY = "ARRAY";

  private List<Element> fieldList = new ArrayList<>();

  public MetadataFieldValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_57);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_57, MODULE_NAME);

    NodeList fields;
    try {
      fields = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:fields/ns:field",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read fields from SIARD file";
      setError(M_571, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    closeZipFile();

    if (fields.getLength() == 0) {
      getValidationReporter().skipValidation(M_571, "Database has no fields");
      observer.notifyValidationStep(MODULE_NAME, M_571, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_571, FIELD_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:fields/ns:field");

    if (validateFieldName(fields)) {
      validationOk(MODULE_NAME, M_571_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_571_1, ValidationReporterStatus.ERROR);
    }

    closeZipFile();

    if (validateType()) {
      validationOk(MODULE_NAME, A_M_571_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_571_2, ValidationReporterStatus.ERROR);
    }

    closeZipFile();

    validateFieldDescription(fields);
    validationOk(MODULE_NAME, A_M_571_5);

    closeZipFile();

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.7-1-1 The field name is mandatory.(SIARD Format Specification)
   */
  private boolean validateFieldName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element field = (Element) nodes.item(i);
      String schemaName = XMLUtils.getParentNameByTagName(field, Constants.SCHEMA);
      String tableName = XMLUtils.getParentNameByTagName(field, Constants.TABLE);
      String columnName = XMLUtils.getParentNameByTagName(field, Constants.COLUMN);
      String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, columnName,
        Constants.FIELD, Integer.toString(i));

      String name = XMLUtils.getChildTextContext(field, Constants.NAME);
      if (!validateXMLField(M_571_1, name, Constants.NAME, true, false, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * find the type of column or field in the SIARD file and perform category and
   * lobFolder validations(M_5.7-1-2)
   */
  private boolean validateType() {
    boolean hasErrors = false;
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
      closeZipFile();

      if (nodes == null) {
        return true;
      }

      for (int i = 0; i < nodes.getLength(); i++) {
        Element column = (Element) nodes.item(i);
        String columnName = XMLUtils.getChildTextContext(column, Constants.NAME);
        String columnTypeName = XMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
        String columnTypeSchema = XMLUtils.getChildTextContext(column, Constants.TYPE_SCHEMA);
        String columnTypeOriginal = XMLUtils.getChildTextContext(column, Constants.TYPE_ORIGINAL);
        String schemaName = XMLUtils.getParentNameByTagName(column, Constants.SCHEMA);
        String tableName = XMLUtils.getParentNameByTagName(column, Constants.TABLE);

        Element fieldsElement = XMLUtils.getChild(column, Constants.FIELDS);
        if (fieldsElement == null) {
          // next column
          continue;
        }
        fieldList.add(fieldsElement);

        String xpathField = String.format(
          "/ns:siardArchive/ns:schemas/ns:schema[ns:name='%s']/ns:tables/ns:table[ns:name='%s']/ns:columns/ns:column[ns:name='%s']/ns:fields/ns:field",
          schemaName, tableName, columnName);

        if (!validateFieldNode(xpathField, schemaName, tableName, columnName, columnTypeName, columnTypeSchema,
          columnTypeOriginal)) {
          hasErrors = true;
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException | ModuleException e) {
      String errorMessage = "Unable to read fields from SIARD file";
      setError(M_571, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return !hasErrors;
  }

  /**
   * Perform validations for each field and makes recursive calls to validate
   * subfields. Return nothing because have to report each error
   */
  private boolean validateFieldNode(String currentXPath, String schemaName, String tableName, String columnName,
    String columnTypeName, String columnTypeSchema, String columnTypeOriginal)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException, ModuleException {
    boolean hasErrors = false;
    // if no subfields exist, break recursive call
    if (!(Boolean) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), currentXPath,
      XPathConstants.BOOLEAN, Constants.NAMESPACE_FOR_METADATA)) {
      return true;
    }
    closeZipFile();

    NodeList fieldNodes = (NodeList) XMLUtils.getXPathResult(
      getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), currentXPath, XPathConstants.NODESET,
      Constants.NAMESPACE_FOR_METADATA);
    closeZipFile();

    // each field will call this method again for find subfields and do the same
    // validations
    for (int j = 0; j < fieldNodes.getLength(); j++) {
      Element field = (Element) fieldNodes.item(j);
      // build path with field index in SIARD for log error if name not exist
      String name = XMLUtils.getChildTextContext(field, Constants.NAME);

      // build path with field name in SIARD for log errors
      String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, columnName,
        Constants.FIELD, name);

      // A_M_5.7-1-2
      String lobFolder = XMLUtils.getChildTextContext(field, Constants.LOB_FOLDER);
      if (!validateType(columnTypeName, columnTypeSchema, columnTypeOriginal, name, lobFolder, path)) {
        hasErrors = true;
      }

      // if the field has subfields, the attribute typeName will exist and point to
      // another type in SIARD, so before making a recursive call, make sure typeName
      // exists and pass to the method
      String attrName = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        String.format(
          "/ns:siardArchive/ns:schemas/ns:schema[ns:name='%s']/ns:types/ns:type[ns:name='%s']/ns:attributes/ns:attribute[ns:name='%s']/ns:typeName/text()",
          columnTypeSchema, columnTypeName, name),
        XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);
      closeZipFile();
      if (attrName.isEmpty()) {
        attrName = columnTypeName;
      }

      // new xpath must have the field name to only retrieve its own subfields
      String newXPath = String.format("%s[ns:name='%s']/ns:fields/ns:field", currentXPath, name);
      if (!validateFieldNode(newXPath, schemaName, tableName, columnName, attrName, columnTypeSchema,
        columnTypeOriginal)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * find the type of column or field in the SIARD file and perform category and
   * lobFolder validations(M_5.7-1-2)
   */
  private boolean validateType(String columnTypeName, String columnTypeSchema, String columnTypeOriginal,
    String fieldName, String lobFolder, String path) {
    // if the originalType exists in the column and if it is an array, no need to
    // validate attribute category
    if (columnTypeOriginal != null && columnTypeOriginal.contains(ARRAY)) {
      return true;
    }
    try {
      String xPathExpression = String.format(
        "/ns:siardArchive/ns:schemas/ns:schema[ns:name='%s']/ns:types/ns:type[ns:name='%s' and ns:attributes/ns:attribute[ns:name='%s']]",
        columnTypeSchema, columnTypeName, fieldName);

      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        xPathExpression, XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);
      closeZipFile();

      if (nodes.getLength() < 1) {
        setError(M_571,
          String.format("Field '%s' does not match with any attributes in the database (%s)", fieldName, path));
        return false;
      }

      for (int i = 0; i < nodes.getLength(); i++) {
        Element typeElement = (Element) nodes.item(i);
        String category = XMLUtils.getChildTextContext(typeElement, Constants.CATEGORY);

        if (!validateTypeCategory(category, columnTypeName, path)) {
          return false;
        }

        xPathExpression = String.format(
          "/ns:siardArchive/ns:schemas/ns:schema[ns:name='%s']/ns:types/ns:type[ns:name='%s']/ns:attributes/ns:attribute[ns:name='%s']",
          columnTypeSchema, columnTypeName, fieldName);

        NodeList attributeNode = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xPathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);
        closeZipFile();

        for (int k = 0; k < attributeNode.getLength(); k++) {
          Element attribute = (Element) attributeNode.item(k);
          String attributeType = XMLUtils.getChildTextContext(attribute, Constants.TYPE);
          String attributeTypeName = XMLUtils.getChildTextContext(attribute, Constants.TYPE_NAME);

          if (!validateFieldLobFolder(lobFolder, attributeType, attributeTypeName, path)) {
            return false;
          }
        }
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException | ModuleException e) {
      String errorMessage = "Unable to read attributes from SIARD file";
      setError(M_571, errorMessage);
      LOGGER.debug(errorMessage, e);
    }
    return true;
  }

  /**
   * A_M_5.7-1-2 The following field metadata are stored in the metadata.xml file
   * if a column or a field is an advanced or structured data type of category
   * “distinct” or “udt”:
   *
   * @return true if valid otherwise false
   */
  private boolean validateTypeCategory(String category, String columnTypeName, String path) {
    if (columnTypeName != null) {
      if (category == null) {
        setError(A_M_571_2, String.format("Type '%s' does not exist on SIARD file (%s)", columnTypeName, path));
        return false;
      } else if (!category.equals(Constants.DISTINCT) && !category.equals(Constants.UDT)) {
        setError(A_M_571_2, String.format("Category of type '%s' must be DISTINCT or UDT (%s)", columnTypeName, path));
        return false;
      }
    } else {
      setError(A_M_571_2, String.format("Column typeName must exist (%s)", path));
      return false;
    }
    return true;
  }

  /**
   * A_M_5.7-1-2 The field lobFolder is mandatory.(SIARD Format Specification)
   *
   * @return true if valid otherwise false
   */
  private boolean validateFieldLobFolder(String lobFolder, String attributeType, String attributeTypeName,
    String path) {
    String type = attributeType != null ? attributeType : attributeTypeName;
    if (type == null) {
      setError(A_M_571_2, String.format("Attribute does not have a type or typeOriginal (%s)", path));
      return false;
    } else if (!type.equals(Constants.CHARACTER_LARGE_OBJECT) && !type.equals(Constants.BINARY_LARGE_OBJECT)
      && !type.equals(Constants.BLOB) && !type.equals(Constants.CLOB) && !type.equals(Constants.XML_LARGE_OBJECT)) {
      return true;
    }

    return validateXMLField(A_M_571_2, lobFolder, Constants.LOB_FOLDER, true, false, path);
  }

  /**
   * A_M_5.7-1-5 The field description in SIARD file should not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateFieldDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element field = (Element) nodes.item(i);
      String schemaName = XMLUtils.getParentNameByTagName(field, Constants.SCHEMA);
      String tableName = XMLUtils.getParentNameByTagName(field, Constants.TABLE);
      String columnName = XMLUtils.getParentNameByTagName(field, Constants.COLUMN);
      String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, columnName,
        Constants.FIELD, XMLUtils.getChildTextContext(field, Constants.NAME));

      String description = XMLUtils.getChildTextContext(field, Constants.DESCRIPTION);
      validateXMLField(A_M_571_5, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
