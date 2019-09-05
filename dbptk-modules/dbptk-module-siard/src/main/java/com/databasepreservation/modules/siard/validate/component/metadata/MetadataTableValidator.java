package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;

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
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataTableValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTableValidator.class);
  private final String MODULE_NAME;
  private static final String M_55 = "5.5";
  private static final String M_551 = "M_5.5-1";
  private static final String M_551_1 = "M_5.5-1-1";
  private static final String A_M_551_1 = "A_M_5.5-1-1";
  private static final String M_551_2 = "M_5.5-1-2";
  private static final String A_M_551_3 = "A_M_5.5-1-3";
  private static final String M_551_4 = "M_5.5-1-4";
  private static final String M_551_10 = "M_5.5-1-10";

  public MetadataTableValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_551, M_551_1, A_M_551_1, M_551_2, A_M_551_3, M_551_4, M_551_10);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_55);
    if (preValidationRequirements()){
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_55, MODULE_NAME);

    validateMandatoryXSDFields(M_551, TABLE_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table");

    if (!readXMLMetadataTable()) {
      reportValidations(M_551, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataTable() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String schema = XMLUtils.getChildTextContext((Element) table.getParentNode().getParentNode(), "name");
        String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(table, Constants.NAME);
        validateTableName(name, path);

        path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, name);

        String folder = XMLUtils.getChildTextContext(table, Constants.FOLDER);
        validateTableFolder(folder, path);

        String description = XMLUtils.getChildTextContext(table, Constants.DESCRIPTION);
        validateTableDescription(description, path);

        String columns = XMLUtils.getChildTextContext(table, Constants.COLUMNS);
        validateTableColumns(columns, path);

        String rows = XMLUtils.getChildTextContext(table, Constants.ROWS);
        validateTableRows(rows, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read tables from SIARD file";
      setError(M_551, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.5-1-1 The table name is mandatory in SIARD 2.1 specification
   */
  private void validateTableName(String name, String path) {
    if(validateXMLField(M_551_1, name, Constants.NAME, true, false, path)){
      validateXMLField(A_M_551_1, name, Constants.NAME, false, true, path);
      return;
    }
    setError(A_M_551_1, String.format("Aborted because table name is mandatory (%s)", path));
  }

  /**
   * M_5.5-1-2 The table folder is mandatory in SIARD 2.1 specification
   */
  private void validateTableFolder(String folder, String path) {
    validateXMLField(M_551_2, folder, Constants.FOLDER, true, false, path);
  }

  /**
   * A_M_5.5-1-3 The table description in SIARD file must not be less than 3
   * characters.
   */
  private void validateTableDescription(String description, String path) {
    validateXMLField(A_M_551_3, description, Constants.DESCRIPTION, false, true, path);
  }

  /**
   * M_5.5-1-4 The table columns is mandatory in SIARD 2.1 specification
   */
  private void validateTableColumns(String columns, String path) {
    validateXMLField(M_551_4, columns, Constants.COLUMNS, true, false, path);
  }

  /**
   * M_5.5-1-10 The table rows is mandatory in SIARD 2.1 specification
   */
  private void validateTableRows(String rows, String path) {
    validateXMLField(M_551_10, rows, Constants.ROWS, true, false, path);
  }
}
