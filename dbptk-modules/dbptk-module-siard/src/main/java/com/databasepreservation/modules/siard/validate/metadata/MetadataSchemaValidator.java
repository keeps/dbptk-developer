package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;

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
public class MetadataSchemaValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Schema level metadata";
  private static final String M_52 = "5.2";
  private static final String M_521 = "M_5.2-1";
  private static final String M_521_1 = "M_5.2-1-1";
  private static final String M_521_2 = "M_5.2-1-2";
  private static final String M_521_4 = "M_5.2-1-4";
  private static final String M_521_5 = "M_5.2-1-5";

  public static MetadataSchemaValidator newInstance() {
    return new MetadataSchemaValidator();
  }

  private MetadataSchemaValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    readXMLMetadataSchemaLevel();

    return reportValidations(M_521) && reportValidations(M_521_1) && reportValidations(M_521_2)
      && reportValidations(M_521_4) && reportValidations(M_521_5);
  }

  /**
   * M_5.2-1 All metadata that are designated as mandatory in metadata.xsd at
   * schema level must be completed accordingly.
   *
   * @return true if valid otherwise false
   */
  private boolean readXMLMetadataSchemaLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);

        String name = MetadataXMLUtils.getChildTextContext(schema, Constants.NAME);
        String folder = MetadataXMLUtils.getChildTextContext(schema, Constants.FOLDER);
        String tables = MetadataXMLUtils.getChildTextContext(schema, Constants.TABLES);
        String description = MetadataXMLUtils.getChildTextContext(schema, Constants.DESCRIPTION);

        // M_5.2-1
        // if ((name == null || name.isEmpty()) || (folder == null || folder.isEmpty()))
        // {
        // return false;
        // }

        if (!validateSchemaName(name) || !validateSchemaFolder(name, folder)
          || !validateSchemaDescription(name, description) || !validateSchemaTable(name, tables)) {
          break;
        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException |

      SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.2-1-1 The schema name in the database must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaName(String name) {
    return validateXMLField(M_521_1, name, Constants.NAME, true, true, Constants.SCHEMA, name);
  }

  /**
   * M_5.2-1-2 The schema folder in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaFolder(String schema, String folder) {
    return validateXMLField(M_521_2, folder, Constants.FOLDER, true, true, Constants.SCHEMA, schema);
  }

  /**
   * M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaDescription(String schema, String description) {
    return validateXMLField(M_521_4, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema);
  }

  /**
   * M_5.2-1-5 The schema tables in the database must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaTable(String schema, String tablesList) {
    return validateXMLField(M_521_5, tablesList, Constants.TABLES, true, false, Constants.SCHEMA, schema);
  }
}
