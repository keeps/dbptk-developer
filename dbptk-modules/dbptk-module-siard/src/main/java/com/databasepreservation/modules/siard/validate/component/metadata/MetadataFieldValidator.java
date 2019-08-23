package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;

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
public class MetadataFieldValidator extends MetadataValidator {

  private static final String MODULE_NAME = "Field level metadata";
  private static final String M_57 = "5.7";
  private static final String M_571 = "M_5.7-1";
  private static final String M_571_5 = "M_5.7-1-5";

  private static final String FIELD = "Field";

  public static MetadataValidator newInstance() {
    return new MetadataFieldValidator();
  }

  private MetadataFieldValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_57, MODULE_NAME);

    if (!readXMLMetadataFieldLevel()) {
      reportValidations(M_571, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_571, MODULE_NAME) && reportValidations(M_571_5, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }

    return false;
  }

  private boolean readXMLMetadataFieldLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:fields/ns:field",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      if (nodes == null) {
        return true;
      }

      for (int i = 0; i < nodes.getLength(); i++) {
        Element field = (Element) nodes.item(i);

        Element columnElement = (Element) field.getParentNode().getParentNode();
        String columnName = MetadataXMLUtils.getChildTextContext(columnElement, Constants.NAME);
        Element tableElement = (Element) columnElement.getParentNode().getParentNode();
        String tableName = MetadataXMLUtils.getChildTextContext(tableElement, Constants.NAME);
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();
        String schemaName = MetadataXMLUtils.getChildTextContext(schemaElement, Constants.NAME);

        String name = MetadataXMLUtils.getChildTextContext(field, Constants.NAME);

        // * M_5.7-1 The field name in SIARD is mandatory.
        if (name == null || name.isEmpty()) {
          setError(M_571, String.format("Field name cannot be null (%s)",
            buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, columnName)));
          return false;
        }

        String path = buildPath(Constants.SCHEMA, schemaName, Constants.TABLE, tableName, Constants.COLUMN, columnName,
          FIELD, name);

        String description = MetadataXMLUtils.getChildTextContext(field, Constants.DESCRIPTION);
        if (!validateFieldDescription(description, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.7-1-5 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateFieldDescription(String description, String path) {
    return validateXMLField(M_571_5, description, Constants.DESCRIPTION, false, true, path);
  }

}
