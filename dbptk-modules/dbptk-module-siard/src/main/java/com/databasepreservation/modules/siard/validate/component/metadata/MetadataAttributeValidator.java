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
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataAttributeValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataAttributeValidator.class);
  private final String MODULE_NAME;
  private static final String M_54 = "5.4";
  private static final String M_541 = "M_5.4-1";
  private static final String M_541_1 = "M_5.4-1-1";
  private static final String A_M_541_8 = "A_M_5.4-1-8";

  private List<Element> attributeList = new ArrayList<>();

  public MetadataAttributeValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_541, M_541_1, A_M_541_8);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_54);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_54, MODULE_NAME);

    validateMandatoryXSDFields(M_541, ATTRIBUTE_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type/ns:attributes/ns:attribute");

    if (!readXMLMetadataAttributeLevel()) {
      reportValidations(M_541, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    // there is no need to continue the validation if no have attributes in any type
    // field of schema
    if (attributeList.isEmpty()) {
      getValidationReporter().skipValidation(M_541, "Database has no attributes");
      observer.notifyValidationStep(MODULE_NAME, M_541, ValidationReporter.Status.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataAttributeLevel() {
    if (preValidationRequirements())
      return false;

    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element type = (Element) nodes.item(i);
        String typeName = XMLUtils.getChildTextContext(type, Constants.NAME);
        String schema = XMLUtils.getChildTextContext((Element) type.getParentNode().getParentNode(), "name");
        String path = buildPath(Constants.SCHEMA, schema, Constants.TYPE, typeName);
        NodeList attributesNode = type.getElementsByTagName(Constants.ATTRIBUTE);
        for (int j = 0; j < attributesNode.getLength(); j++) {
          Element attribute = (Element) attributesNode.item(j);
          attributeList.add(attribute);

          String attributeName = XMLUtils.getChildTextContext(attribute, Constants.NAME);
          String description = XMLUtils.getChildTextContext(attribute, Constants.DESCRIPTION);

          validateAttributeName(attributeName, path);
          validateAttributeDescription(description, path + buildPath(Constants.ATTRIBUTE, attributeName));
        }
      }

    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      String errorMessage = "Unable to read attributes from SIARD file";
      setError(M_541, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.4-1-1 The attribute name is mandatory in SIARD 2.1 specification
   */
  private void validateAttributeName(String name, String path) {
    validateXMLField(M_541_1, name, Constants.NAME, true, false, path);
  }

  /**
   * A_M_5.4-1-8 The attribute description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateAttributeDescription(String description, String path) {
    validateXMLField(A_M_541_8, description, Constants.DESCRIPTION, false, true, path);
  }
}
