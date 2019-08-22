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

import com.databasepreservation.Constants;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataTypeValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Type level metadata";
  private static final String M_53 = "5.3";
  private static final String M_531 = "M_5.3-1";
  private static final String M_531_1 = "M_5.3-1-1";
  private static final String M_531_2 = "M_5.3-1-2";
  private static final String M_531_5 = "M_5.3-1-5";
  private static final String M_531_6 = "M_5.3-1-6";
  private static final String M_531_10 = "M_5.3-1-10";

  private static final String TYPE_CATEGORY = "category";
  private static final String TYPE_INSTANTIABLE = "instantiable";
  private static final String TYPE_FINAL = "final";

  private List<Element> typesList = new ArrayList<>();

  public static MetadataTypeValidator newInstance() {
    return new MetadataTypeValidator();
  }

  private MetadataTypeValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_53, MODULE_NAME);

    if (!readXMLMetadataTypeLevel()) {
      return reportValidations(M_531);
    }

    // there is no need to continue the validation if no have types in any schema
    if (typesList.isEmpty()) {
      return true;
    }

    return reportValidations(M_531_1) && reportValidations(M_531_2) && reportValidations(M_531_5)
      && reportValidations(M_531_6) && reportValidations(M_531_10);
  }

  private boolean readXMLMetadataTypeLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element type = (Element) nodes.item(i);
        typesList.add(type);

        String schema = MetadataXMLUtils.getChildTextContext((Element) type.getParentNode().getParentNode(), "name");

        String name = MetadataXMLUtils.getChildTextContext(type, Constants.NAME);
        String category = MetadataXMLUtils.getChildTextContext(type, TYPE_CATEGORY);
        String instantiable = MetadataXMLUtils.getChildTextContext(type, TYPE_INSTANTIABLE);
        String finalField = MetadataXMLUtils.getChildTextContext(type, TYPE_FINAL);

        String description = MetadataXMLUtils.getChildTextContext(type, Constants.DESCRIPTION);
        if (!validateTypeName(schema, name) || !validateTypeCategory(schema, name, category)
          || !validateTypeInstantiable(schema, name, instantiable) || !validateTypefinal(schema, name, finalField)
          || !validateTypeDescription(schema, name, description)) {
          break;
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.3-1-1 The type name in the schema must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTypeName(String schema, String typeName) {
    return validateXMLField(M_531_1, typeName, Constants.NAME, true, false, Constants.SCHEMA, schema);
  }

  /**
   * M_5.3-1-2 The type category in the schema must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTypeCategory(String schema, String typeName, String category) {
    return validateXMLField(M_531_2, category, TYPE_CATEGORY, true, false, Constants.SCHEMA, schema, Constants.TYPE,
      typeName);
  }

  /**
   * M_5.3-1-5 The type instantiable field in the schema must not be empty. ERROR
   * when it is empty
   *
   * @return true if valid otherwise false
   */

  private boolean validateTypeInstantiable(String schema, String typeName, String instantiable) {
    return validateXMLField(M_531_5, instantiable, TYPE_INSTANTIABLE, true, false, Constants.SCHEMA, schema,
      Constants.TYPE, typeName);
  }

  /**
   * M_5.3-1-6 The type final field in the schema must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTypefinal(String schema, String typeName, String typeFinal) {
    return validateXMLField(M_531_6, typeFinal, TYPE_FINAL, true, false, Constants.SCHEMA, schema, Constants.TYPE,
      typeName);
  }

  /**
   * M_5.3-1-10 The type description field in the schema must not be must not be
   * less than 3 characters. WARNING if it is less than 3 characters
   *
   */
  private boolean validateTypeDescription(String schema, String typeName, String description) {
    return validateXMLField(M_531_10, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.TYPE, typeName);
  }

}
