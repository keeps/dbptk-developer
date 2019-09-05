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
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataSchemaValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataSchemaValidator.class);
  private final String MODULE_NAME;
  private static final String M_52 = "5.2";
  private static final String M_521 = "M_5.2-1";
  private static final String M_521_1 = "M_5.2-1-1";
  private static final String A_M_521_1 = "A_M_5.2-1-1";
  private static final String M_521_2 = "M_5.2-1-2";
  private static final String A_M_521_2 = "A_M_5.2-1-2";
  private static final String A_M_521_4 = "A_M_5.2-1-4";

  public MetadataSchemaValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_521, M_521_1, A_M_521_1, M_521_2, A_M_521_2, A_M_521_4);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_52);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    validateMandatoryXSDFields(M_521, SCHEMA_TYPE, "/ns:siardArchive/ns:schemas/ns:schema");

    if (!readXMLMetadataSchemaLevel()) {
      reportValidations(M_521, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    return reportValidations(MODULE_NAME);
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
        String path = buildPath(Constants.SCHEMA, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(schema, Constants.NAME);
        validateSchemaName(name, path);

        path = buildPath(Constants.SCHEMA, name);
        String folder = XMLUtils.getChildTextContext(schema, Constants.FOLDER);
        validateSchemaFolder(folder, path);

        String description = XMLUtils.getChildTextContext(schema, Constants.DESCRIPTION);
        validateSchemaDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read schema from SIARD file";
      setError(M_521, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.2-1-1 Schema Name is mandatory in SIARD 2.1 specification
   * 
   * A_M_521_1 The schema name in the database must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   */
  private void validateSchemaName(String name, String path) {
    if(validateXMLField(M_521_1, name, Constants.NAME, true, false, path)){
      validateXMLField(A_M_521_1, name, Constants.NAME, false, true, path);
      return;
    }
    setError(A_M_521_1, String.format("Aborted because schema name is mandatory (%s)", path));
  }

  /**
   * M_5.2-1-2 Schema Folder is mandatory in SIARD 2.1 specification
   *
   * A_M_5.2-1-2 The schema folder in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   */
  private void validateSchemaFolder(String folder, String path) {
    if(validateXMLField(M_521_2, folder, Constants.FOLDER, true, false, path)){
      validateXMLField(A_M_521_2, folder, Constants.FOLDER, false, true, path);
      return;
    }
    setError(A_M_521_2, String.format("Aborted because schema folder is mandatory (%s)", path));
  }

  /**
   * A_M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   */
  private void validateSchemaDescription(String description, String path) {
    validateXMLField(A_M_521_4, description, Constants.DESCRIPTION, false, true, path);
  }
}
