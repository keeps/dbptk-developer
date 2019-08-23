package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class MetadataAttributeValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Attribute level metadata";
  private static final String M_54 = "5.4";
  private static final String M_541 = "M_5.4-1";
  private static final String M_541_1 = "M_5.4-1-1";
  private static final String M_541_8 = "M_5.4-1-8";

  private List<Element> attributeList = new ArrayList<>();

  public static MetadataAttributeValidator newInstance() {
    return new MetadataAttributeValidator();
  }

  private MetadataAttributeValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_54, MODULE_NAME);

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
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }

    if (reportValidations(M_541_1, MODULE_NAME) && reportValidations(M_541_8, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }

    return false;
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
        String typeName = MetadataXMLUtils.getChildTextContext(type, Constants.NAME);
        String schema = MetadataXMLUtils.getChildTextContext((Element) type.getParentNode().getParentNode(), "name");
        String path = buildPath(Constants.SCHEMA, schema, Constants.TYPE, typeName);
        NodeList attributesNode = type.getElementsByTagName(Constants.ATTRIBUTE);
        for (int j = 0; j < attributesNode.getLength(); j++) {
          Element attribute = (Element) attributesNode.item(j);
          attributeList.add(attribute);

          String attributeName = MetadataXMLUtils.getChildTextContext(attribute, Constants.NAME);
          String description = MetadataXMLUtils.getChildTextContext(attribute, Constants.DESCRIPTION);

          if (!validateAttributeName(attributeName, path)
            || !validateAttributeDescription(description, path + buildPath(Constants.ATTRIBUTE, attributeName))) {
            break;
          }
        }
      }

    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.4-1-1 The attribute name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateAttributeName(String name, String path) {
    return validateXMLField(M_541_1, name, Constants.NAME, true, false, path);
  }

  /**
   * M_5.4-1-8 The attribute description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateAttributeDescription(String description, String path) {
    return validateXMLField(M_541_8, description, Constants.DESCRIPTION, false, true, path);
  }
}
