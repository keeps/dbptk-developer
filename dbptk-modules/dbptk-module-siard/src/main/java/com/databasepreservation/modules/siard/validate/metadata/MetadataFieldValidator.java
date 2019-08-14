package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataFieldValidator extends MetadataValidator {

  private static final String MODULE_NAME = "Field level metadata";
  private static final String M_57 = "5.7";
  private static final String M_571 = "M_5.7-1";
  private static final String M_571_5 = "M_5.7-1-5";

  private List<Element> fieldList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();

  public static MetadataValidator newInstance() {
    return new MetadataFieldValidator();
  }

  private MetadataFieldValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_57, MODULE_NAME);

    if (!reportValidations(readXMLMetadataFieldLevel(), M_571, true)) {
      return false;
    }

    if (!reportValidations(validateFieldDescription(), M_571_5, true)) {
      return false;
    }

    return true;
  }

  private boolean readXMLMetadataFieldLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:fields/ns:field";
      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      if (nodes == null) {
        return true;
      }

      for (int i = 0; i < nodes.getLength(); i++) {
        Element field = (Element) nodes.item(i);

        Element columnElement = (Element) field.getParentNode().getParentNode();
        String columnName = MetadataXMLUtils.getChildTextContext(columnElement, "name");
        Element tableElement = (Element) columnElement.getParentNode().getParentNode();
        String tableName = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();
        String schemaName = MetadataXMLUtils.getChildTextContext(schemaElement, "name");

        fieldList.add(field);
        String name = MetadataXMLUtils.getChildTextContext(field, "name");

        // * M_5.7-1 The field name in SIARD is mandatory.
        if (name == null || name.isEmpty()) {
          hasErrors = "Field name cannot be null on " + MetadataXMLUtils.createPath(schemaName, tableName, columnName);
          return false;
        }

        descriptionList.add(MetadataXMLUtils.getChildTextContext(field, "description"));
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
  private boolean validateFieldDescription() {
    validateXMLFieldSizeList(descriptionList, "description");
    return true;
  }

}
