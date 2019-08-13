package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

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
  private List<String> attributeName = new ArrayList<>();
  private List<String> attributeDescription = new ArrayList<>();

  public static MetadataAttributeValidator newInstance() {
    return new MetadataAttributeValidator();
  }

  private MetadataAttributeValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_54, MODULE_NAME);

    if (!reportValidations(readXMLMetadataAttributeLevel(), M_541, true)) {
      return false;
    }

    // there is no need to continue the validation if no have attributes in any type
    // field of schema
    if (attributeList.isEmpty()) {
      return true;
    }
    if (!reportValidations(validateAttributeName(), M_541_1, true)) {
      return false;
    }
    if (!reportValidations(validateAttributeDescription(), M_541_8, false)) {
      return false;
    }

    return false;
  }

  private boolean readXMLMetadataAttributeLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final ZipArchiveEntry metadataEntry = zipFile.getEntry("header/metadata.xml");
      final InputStream inputStream = zipFile.getInputStream(metadataEntry);
      Document document = MetadataXMLUtils.getDocument(inputStream);
      String xpathExpressionDatabase = "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type/ns:attributes/ns:attribute";

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionDatabase);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          Element attribute = (Element) nodes.item(i);
          attributeList.add(attribute);

          Element attributeNameElement = MetadataXMLUtils.getChild(attribute, "name");
          String name = attributeNameElement != null ? attributeNameElement.getTextContent() : null;
          attributeName.add(name);

          Element attributeDescriptionElement = MetadataXMLUtils.getChild(attribute, "description");
          String description = attributeDescriptionElement != null ? attributeDescriptionElement.getTextContent()
            : null;
          attributeDescription.add(description);

        }
      } catch (XPathExpressionException e) {
        return false;
      }

    } catch (IOException | ParserConfigurationException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.4-1-1 The attribute name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateAttributeName() {
    return validateMandatoryXMLFieldList(attributeName, "name", false);
  }

  /**
   * M_5.4-1-8 The attribute description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateAttributeDescription() {
    validateXMLFieldSizeList(attributeDescription, "description");
    return true;
  }
}
