package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;

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
public class MetadataSchemaValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Schema level metadata";
  private static final String M_52 = "5.2";
  private static final String M_521 = "M_5.2-1";
  private static final String M_521_1 = "M_5.2-1-1";
  private static final String M_521_2 = "M_5.2-1-2";
  private static final String M_521_4 = "M_5.2-1-4";
  private static final String M_521_5 = "M_5.2-1-5";

  private static final String SCHEMA = "schema";
  private static final String SCHEMA_NAME = "name";
  private static final String SCHEMA_FOLDER = "folder";
  private static final String SCHEMA_TABLES = "tables";
  private static final String SCHEMA_DESCRIPTION = "description";

  public static MetadataSchemaValidator newInstance() {
    return new MetadataSchemaValidator();
  }

  private MetadataSchemaValidator() {
    error.clear();
    warnings.clear();
    warnings.put(SCHEMA_NAME, new ArrayList<String>());
    warnings.put(SCHEMA_FOLDER, new ArrayList<String>());
    warnings.put(SCHEMA_TABLES, new ArrayList<String>());
    warnings.put(SCHEMA_DESCRIPTION, new ArrayList<String>());
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    return reportValidations(readXMLMetadataSchemaLevel(), M_521, true) && reportValidations(M_521_1, SCHEMA_NAME)
      && reportValidations(M_521_2, SCHEMA_FOLDER) && reportValidations(M_521_4, SCHEMA_DESCRIPTION)
      && reportValidations(M_521_5, SCHEMA_TABLES);
  }

  /**
   * M_5.2-1 All metadata that are designated as mandatory in metadata.xsd at
   * schema level must be completed accordingly.
   *
   * @return true if valid otherwise false
   */
  private boolean readXMLMetadataSchemaLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element schema = (Element) nodes.item(i);

        String name = MetadataXMLUtils.getChildTextContext(schema, "name");
        String folder = MetadataXMLUtils.getChildTextContext(schema, "folder");
        String tables = MetadataXMLUtils.getChildTextContext(schema, "tables");
        String description = MetadataXMLUtils.getChildTextContext(schema, "description");

        // M_5.2-1
//        if ((name == null || name.isEmpty()) || (folder == null || folder.isEmpty())) {
//          return false;
//        }

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
    return validateXMLField(name, SCHEMA_NAME, true, true, SCHEMA, name);
  }

  /**
   * M_5.2-1-2 The schema folder in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaFolder(String schema, String folder) {
    return validateXMLField(folder, SCHEMA_FOLDER, true, true, SCHEMA, schema);
  }

  /**
   * M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaDescription(String schema, String description) {
    return validateXMLField(description, SCHEMA_DESCRIPTION, false, true, SCHEMA, schema);
  }

  /**
   * M_5.2-1-5 The schema tables in the database must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaTable(String schema, String tablesList) {
    return validateXMLField(tablesList, SCHEMA_TABLES, true, false, SCHEMA, schema);
  }
}
