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
public class MetadataTableValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Table level metadata";
  private static final String M_55 = "5.5";
  private static final String M_551 = "M_5.5-1";
  private static final String M_551_1 = "M_5.5-1-1";
  private static final String M_551_2 = "M_5.5-1-2";
  private static final String M_551_3 = "M_5.5-1-3";
  private static final String M_551_4 = "M_5.5-1-4";
  private static final String M_551_10 = "M_5.5-1-10";

  public static MetadataTableValidator newInstance() {
    return new MetadataTableValidator();
  }

  private MetadataTableValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_55, MODULE_NAME);

    readXMLMetadataTable();

    if (readXMLMetadataTable()) {
      reportValidations(M_551, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_551, MODULE_NAME) && reportValidations(M_551_1, MODULE_NAME)
      && reportValidations(M_551_2, MODULE_NAME) && reportValidations(M_551_3, MODULE_NAME)
      && reportValidations(M_551_4, MODULE_NAME) && reportValidations(M_551_10, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataTable() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String schema = XMLUtils.getChildTextContext((Element) table.getParentNode().getParentNode(), "name");

        String name = XMLUtils.getChildTextContext(table, Constants.NAME);

        String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, name);
        if (!validateTableName(name, path))
          break;

        String folder = XMLUtils.getChildTextContext(table, Constants.FOLDER);
        if (!validateTableFolder(folder, path))
          break;

        String description = XMLUtils.getChildTextContext(table, Constants.DESCRIPTION);
        if (!validateTableDescription(description, path))
          break;

        String columns = XMLUtils.getChildTextContext(table, Constants.COLUMNS);
        if (!validateTableColumns(columns, path))
          break;

        String rows = XMLUtils.getChildTextContext(table, Constants.ROWS);
        if (!validateTableRows(rows, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.5-1-1 The table name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableName(String name, String path) {
    return validateXMLField(M_551_1, name, Constants.NAME, true, true, path);
  }

  /**
   * M_5.5-1-2 The table folder in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableFolder(String folder, String path) {
    return validateXMLField(M_551_2, folder, Constants.FOLDER, true, false, path);
  }

  /**
   * M_5.5-1-3 The table description in SIARD file must not be less than 3
   * characters.
   *
   */
  private boolean validateTableDescription(String description, String path) {
    return validateXMLField(M_551_3, description, Constants.DESCRIPTION, false, true, path);
  }

  /**
   * M_5.5-1-4 The table columns in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableColumns(String columns, String path) {
    return validateXMLField(M_551_4, columns, Constants.COLUMNS, true, false, path);
  }

  /**
   * M_5.5-1-10 The table rows in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableRows(String rows, String path) {
    return validateXMLField(M_551_10, rows, Constants.ROWS, true, false, path);
  }
}
