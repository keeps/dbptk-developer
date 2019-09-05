package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import com.databasepreservation.model.reporters.ValidationReporter;
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
public class MetadataTypeValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTypeValidator.class);
  private final String MODULE_NAME;
  private static final String M_53 = "5.3";
  private static final String M_531 = "M_5.3-1";
  private static final String M_531_1 = "M_5.3-1-1";
  private static final String M_531_2 = "M_5.3-1-2";
  private static final String M_531_5 = "M_5.3-1-5";
  private static final String M_531_6 = "M_5.3-1-6";
  private static final String A_M_531_10 = "A_M_5.3-1-10";

  private List<Element> typesList = new ArrayList<>();

  public MetadataTypeValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_531, M_531_1, M_531_2, M_531_5, M_531_6, A_M_531_10);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_53);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_53, MODULE_NAME);

    validateMandatoryXSDFields(M_531, TYPE_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type");

    if (!readXMLMetadataTypeLevel()) {
      reportValidations(M_531, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    // there is no need to continue the validation if no have types in any schema
    if (typesList.isEmpty()) {
      getValidationReporter().skipValidation(M_531, "Database has no types");
      observer.notifyValidationStep(MODULE_NAME, M_531, ValidationReporter.Status.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataTypeLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element type = (Element) nodes.item(i);
        typesList.add(type);

        String schema = XMLUtils.getChildTextContext((Element) type.getParentNode().getParentNode(), "name");

        String name = XMLUtils.getChildTextContext(type, Constants.NAME);
        String category = XMLUtils.getChildTextContext(type, Constants.CATEGORY);
        String instantiable = XMLUtils.getChildTextContext(type, Constants.TYPE_INSTANTIABLE);
        String finalField = XMLUtils.getChildTextContext(type, Constants.TYPE_FINAL);
        String description = XMLUtils.getChildTextContext(type, Constants.DESCRIPTION);

        String path = buildPath(Constants.SCHEMA, schema, Constants.TYPE, Integer.toString(i));
        validateTypeName(name, path);

        path = buildPath(Constants.SCHEMA, schema, Constants.TYPE, name);
        validateTypeCategory(category, path);
        validateTypeInstantiable(instantiable, path);
        validateTypeFinal(finalField, path);
        validateTypeDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read types from SIARD file";
      setError(M_531, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.3-1-1 The type name is mandatory in SIARD 2.1 specification
   */
  private void validateTypeName(String typeName, String path) {
    validateXMLField(M_531_1, typeName, Constants.NAME, true, false, path);
  }

  /**
   * M_5.3-1-2 The type category is mandatory in SIARD 2.1 specification
   *
   * must be distinct or udt
   */
  private void validateTypeCategory(String category, String path) {
    if(category == null || category.isEmpty()){
      setError(M_531_2, buildErrorMessage(Constants.CATEGORY, path));
    } else if (!category.equals(Constants.DISTINCT) && !category.equals(Constants.UDT)){
      setError(M_531_2, String.format("type category must be 'distinct' or 'udt' (%s)", path));
    }
  }

  /**
   * M_5.3-1-5 The type instantiable is mandatory in SIARD 2.1 specification
   *
   * must be true or false
   */
  private void validateTypeInstantiable(String instantiable, String path) {
    if(instantiable == null || instantiable.isEmpty()){
      setError(M_531_5, buildErrorMessage(Constants.TYPE_INSTANTIABLE, path));
    } else if (!instantiable.equals(Constants.TRUE) && !instantiable.equals(Constants.FALSE)){
      setError(M_531_5, String.format("type instantiable must be 'true' or 'false' (%s)", path));
    }
  }

  /**
   * M_5.3-1-6 The type final field is mandatory in SIARD 2.1 specification
   *
   * must be true or false
   */
  private void validateTypeFinal(String typeFinal, String path) {
    if(typeFinal == null || typeFinal.isEmpty()){
      setError(M_531_6, buildErrorMessage(Constants.TYPE_INSTANTIABLE, path));
    } else if (!typeFinal.equals(Constants.TRUE) && !typeFinal.equals(Constants.FALSE)) {
      setError(M_531_6, String.format("type final must be 'true' or 'false' (%s)", path));
    }
  }

  /**
   * A_M_5.3-1-10 The type description field in the schema must not be must not be
   * less than 3 characters. WARNING if it is less than 3 characters
   */
  private void validateTypeDescription(String description, String path) {
    validateXMLField(A_M_531_10, description, Constants.DESCRIPTION, false, true, path);
  }

}
