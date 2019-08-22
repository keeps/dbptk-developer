package com.databasepreservation.modules.siard.validate.metadata;

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
public class MetadataFieldValidator extends MetadataValidator {

  private static final String MODULE_NAME = "Field level metadata";
  private static final String M_57 = "5.7";
  private static final String M_571 = "M_5.7-1";
  private static final String M_571_5 = "M_5.7-1-5";

  private static final String FIELD = "Field";

  public static MetadataValidator newInstance() {
    return new MetadataFieldValidator();
  }

  private MetadataFieldValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_57, MODULE_NAME);
    readXMLMetadataFieldLevel();

    return reportValidations(M_571) && reportValidations(M_571_5);
  }

  private boolean readXMLMetadataFieldLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column/ns:fields/ns:field";
      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      if (nodes == null) {
        return true;
      }

      for (int i = 0; i < nodes.getLength(); i++) {
        Element field = (Element) nodes.item(i);

        Element columnElement = (Element) field.getParentNode().getParentNode();
        String columnName = MetadataXMLUtils.getChildTextContext(columnElement, Constants.NAME);
        Element tableElement = (Element) columnElement.getParentNode().getParentNode();
        String tableName = MetadataXMLUtils.getChildTextContext(tableElement, Constants.NAME);
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();
        String schemaName = MetadataXMLUtils.getChildTextContext(schemaElement, Constants.NAME);

        String name = MetadataXMLUtils.getChildTextContext(field, Constants.NAME);

        // * M_5.7-1 The field name in SIARD is mandatory.
        if (name == null || name.isEmpty()) {
          setError(M_571,
            "Field name cannot be null on " + MetadataXMLUtils.createPath(schemaName, tableName, columnName));
          return false;
        }

        String description = MetadataXMLUtils.getChildTextContext(field, Constants.DESCRIPTION);
        if (!validateFieldDescription(schemaName, tableName, columnName, name, description))
          break;
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
  private boolean validateFieldDescription(String schema, String table, String column, String field,
    String description) {
    return validateXMLField(M_571_5, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.TABLE, table, Constants.COLUMN, column, FIELD, field);
  }

}
