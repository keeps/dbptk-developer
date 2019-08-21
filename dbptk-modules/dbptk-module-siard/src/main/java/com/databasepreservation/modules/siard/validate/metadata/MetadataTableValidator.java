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
public class MetadataTableValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Table level metadata";
  private static final String M_55 = "5.5";
  private static final String M_551 = "M_5.5-1";
  private static final String M_551_1 = "M_5.5-1-1";
  private static final String M_551_2 = "M_5.5-1-2";
  private static final String M_551_3 = "M_5.5-1-3";
  private static final String M_551_4 = "M_5.5-1-4";
  private static final String M_551_10 = "M_5.5-1-10";

  public static MetadataTableValidator newInstance() {
    return new MetadataTableValidator();
  }

  private MetadataTableValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_55, MODULE_NAME);

    readXMLMetadataTable();

    return reportValidations(M_551) && reportValidations(M_551_1) && reportValidations(M_551_2)
      && reportValidations(M_551_3) && reportValidations(M_551_4) && reportValidations(M_551_10);
  }

  private boolean readXMLMetadataTable() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);
      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String schema = MetadataXMLUtils.getChildTextContext((Element) table.getParentNode().getParentNode(), "name");

        String name = MetadataXMLUtils.getChildTextContext(table, Constants.NAME);
        if (!validateTableName(schema, name))
          break;

        String folder = MetadataXMLUtils.getChildTextContext(table, Constants.FOLDER);
        if (!validateTableFolder(schema, name, folder))
          break;

        String description = MetadataXMLUtils.getChildTextContext(table, Constants.DESCRIPTION);
        if (!validateTableDescription(schema, name, description))
          break;

        String columns = MetadataXMLUtils.getChildTextContext(table, Constants.COLUMNS);
        if (!validateTableColumns(schema, name, columns))
          break;

        String rows = MetadataXMLUtils.getChildTextContext(table, Constants.ROWS);
        if (!validateTableRows(schema, name, rows))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.5-1-1 The table name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableName(String schema, String name) {
    return validateXMLField(M_551_1, name, Constants.NAME, true, true, Constants.SCHEMA, schema, Constants.TABLE, name);
  }

  /**
   * M_5.5-1-2 The table folder in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableFolder(String schema, String name, String folder) {
    return validateXMLField(M_551_2, folder, Constants.FOLDER, true, false, Constants.SCHEMA, schema, Constants.TABLE,
      name);
  }

  /**
   * M_5.5-1-3 The table description in SIARD file must not be less than 3
   * characters.
   *
   */
  private boolean validateTableDescription(String schema, String name, String description) {
    return validateXMLField(M_551_3, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.TABLE, name);
  }

  /**
   * M_5.5-1-4 The table columns in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableColumns(String schema, String name, String columns) {
    return validateXMLField(M_551_4, columns, Constants.COLUMNS, true, false, Constants.SCHEMA, schema, Constants.TABLE,
      name);
  }

  /**
   * M_5.5-1-10 The table rows in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableRows(String schema, String name, String rows) {
    return validateXMLField(M_551_10, rows, Constants.ROWS, true, false, Constants.SCHEMA, schema, Constants.TABLE,
      name);
  }
}
