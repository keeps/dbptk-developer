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
public class MetadataSchemaValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Schema level metadata";
  private static final String M_52 = "5.2";
  private static final String M_521 = "M_5.2-1";
  private static final String M_521_1 = "M_5.2-1-1";
  private static final String M_521_2 = "M_5.2-1-2";
  private static final String M_521_4 = "M_5.2-1-4";
  private static final String M_521_5 = "M_5.2-1-5";

  public static MetadataSchemaValidator newInstance() {
    return new MetadataSchemaValidator();
  }

  private MetadataSchemaValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    if (!readXMLMetadataSchemaLevel()) {
      reportValidations(M_521, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_521_1, MODULE_NAME) && reportValidations(M_521_2, MODULE_NAME)
      && reportValidations(M_521_4, MODULE_NAME) && reportValidations(M_521_5, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  /**
   * M_5.2-1 All metadata that are designated as mandatory in metadata.xsd at
   * schema level must be completed accordingly.
   *
   * @return true if valid otherwise false
   */
  private boolean readXMLMetadataSchemaLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);
        String name = XMLUtils.getChildTextContext(schema, Constants.NAME);
        String folder = XMLUtils.getChildTextContext(schema, Constants.FOLDER);
        String tables = XMLUtils.getChildTextContext(schema, Constants.TABLES);
        String description = XMLUtils.getChildTextContext(schema, Constants.DESCRIPTION);

        // M_5.2-1
        // if ((name == null || name.isEmpty()) || (folder == null || folder.isEmpty()))
        // {
        // return false;
        // }

        String path = buildPath(Constants.SCHEMA, name);

        if (!validateSchemaName(name, path) || !validateSchemaFolder(folder, path)
          || !validateSchemaDescription(description, path) || !validateSchemaTable(tables, path)) {
          break;
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException |

      SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.2-1-1 The schema name in the database must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaName(String name, String path) {
    return validateXMLField(M_521_1, name, Constants.NAME, true, true, path);
  }

  /**
   * M_5.2-1-2 The schema folder in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaFolder(String folder, String path) {
    return validateXMLField(M_521_2, folder, Constants.FOLDER, true, true, path);
  }

  /**
   * M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaDescription(String description, String path) {
    return validateXMLField(M_521_4, description, Constants.DESCRIPTION, false, true, path);
  }

  /**
   * M_5.2-1-5 The schema tables in the database must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaTable(String tablesList, String path) {
    return validateXMLField(M_521_5, tablesList, Constants.TABLES, true, false, path);
  }
}
