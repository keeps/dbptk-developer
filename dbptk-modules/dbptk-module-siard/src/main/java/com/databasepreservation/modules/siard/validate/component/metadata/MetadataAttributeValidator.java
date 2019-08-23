package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import com.databasepreservation.Constants;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_54, MODULE_NAME);

    if (!readXMLMetadataAttributeLevel()) {
      return reportValidations(M_541);
    }

    // there is no need to continue the validation if no have attributes in any type
    // field of schema
    if (attributeList.isEmpty()) {
      return true;
    }

    return reportValidations(M_541_1) && reportValidations(M_541_8);
  }

  private boolean readXMLMetadataAttributeLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element type = (Element) nodes.item(i);
        String typeName = MetadataXMLUtils.getChildTextContext(type, Constants.NAME);
        String schema = MetadataXMLUtils.getChildTextContext((Element) type.getParentNode().getParentNode(), "name");

        NodeList attributesNode = type.getElementsByTagName(Constants.ATTRIBUTE);
        for (int j = 0; j < attributesNode.getLength(); j++) {
          Element attribute = (Element) attributesNode.item(j);
          attributeList.add(attribute);

          String attributeName = MetadataXMLUtils.getChildTextContext(attribute, Constants.NAME);
          String description = MetadataXMLUtils.getChildTextContext(attribute, Constants.DESCRIPTION);

          if (!validateAttributeName(schema, typeName, attributeName)
            || !validateAttributeDescription(schema, typeName, attributeName, description)) {
            break;
          }
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.4-1-1 The attribute name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateAttributeName(String schema, String type, String name) {
    return validateXMLField(M_541_1, name, Constants.NAME, true, false, Constants.SCHEMA, schema, Constants.TYPE, type);
  }

  /**
   * M_5.4-1-8 The attribute description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateAttributeDescription(String schema, String type, String attributeName, String description) {
    return validateXMLField(M_541_8, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.TYPE, type, Constants.ATTRIBUTE, attributeName);
  }
}
