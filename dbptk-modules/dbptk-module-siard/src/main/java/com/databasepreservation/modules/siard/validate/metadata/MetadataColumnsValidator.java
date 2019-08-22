package com.databasepreservation.modules.siard.validate.metadata;

import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.BINARY_LARGE_OBJECT;
import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.CHARACTER_LARGE_OBJECT;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.databasepreservation.Constants;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.modules.siard.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataColumnsValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Column level metadata";
  private static final String M_56 = "5.6";
  private static final String M_561 = "M_5.6-1";
  private static final String M_561_1 = "M_5.6-1-1";
  private static final String M_561_3 = "M_5.6-1-3";
  private static final String M_561_5 = "M_5.6-1-5";
  private static final String M_561_12 = "M_5.6-1-12";
  private static final String BLOB = "BLOB";
  private static final String CLOB = "CLOB";
  private static final String XML = "XML";


  private Set<String> typeOriginalSet = new HashSet<>();

  public static ValidatorModule newInstance() {
    return new MetadataColumnsValidator();
  }

  private MetadataColumnsValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);
    readXMLMetadataColumnLevel();

    return reportValidations(M_561) && reportValidations(M_561_1)
      && reportValidations(M_561_3) && noticeTypeOriginalUsed()
      && reportValidations(M_561_12);
  }

  private boolean readXMLMetadataColumnLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);
      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String tableFolderName = MetadataXMLUtils.getChildTextContext(table, Constants.FOLDER);
        String tableName = MetadataXMLUtils.getChildTextContext(table, Constants.NAME);

        Element schemaElement = (Element) table.getParentNode().getParentNode();
        String schemaFolderName = MetadataXMLUtils.getChildTextContext(schemaElement, Constants.FOLDER);
        String schemaName = MetadataXMLUtils.getChildTextContext(schemaElement, Constants.NAME);

        Element columns = ((Element) table.getElementsByTagName(Constants.COLUMNS).item(0));
        NodeList columnNodes = columns.getElementsByTagName(Constants.COLUMN);

        for (int j = 0; j < columnNodes.getLength(); j++) {
          Element column = (Element) columnNodes.item(j);

          // * M_5.6-1 The column name in SIARD is mandatory.
          String name = MetadataXMLUtils.getChildTextContext(column, Constants.NAME);
          if (!validateColumnName(schemaName, tableName, name))
            break;

          // * M_5.6-1 The column type in SIARD is mandatory.
          String type = MetadataXMLUtils.getChildTextContext(column, Constants.TYPE);
          if (type == null || type.isEmpty()) {
            String typeName = MetadataXMLUtils.getChildTextContext(column, Constants.TYPE_NAME);
            if (typeName == null || typeName.isEmpty()) {
              setError(M_561, "Column type cannot be null schema: " + schemaFolderName + " table: "
                + tableFolderName + " column: " + name);
              return false;
            }
            // TODO check typeName in SIARD types node
            type = typeName;
          }

          if (type.equals(CHARACTER_LARGE_OBJECT) || type.equals(BINARY_LARGE_OBJECT) || type.equals(BLOB)
            || type.equals(CLOB) || type.equals(XML)) {

            String folder = MetadataXMLUtils.getChildTextContext(column, Constants.LOB_FOLDER);
            String columnNumber = "c" + (j + 1);
            if (!validateColumnLobFolder(schemaFolderName, tableFolderName, type, folder, columnNumber, name))
              break;
          }

          typeOriginalSet.add(MetadataXMLUtils.getChildTextContext(column, Constants.TYPE_ORIGINAL));
          String description = MetadataXMLUtils.getChildTextContext(column, Constants.DESCRIPTION);
          if (!validateColumnDescription(schemaName, tableName, description))
            break;

        }
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.6-1-1 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnName(String schema, String table, String name) {
    return validateXMLField(M_561_1, name, Constants.NAME, true, false, Constants.SCHEMA, schema, Constants.TABLE, table);
  }

  /**
   * M_5.6-1-3 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnLobFolder(String schemaFolder, String tableFolder, String type, String folder,
    String column, String name) {
    String path = MetadataXMLUtils.createPath(MetadataXMLUtils.SIARD_CONTENT, schemaFolder, tableFolder);
    if (!HasReferenceToLobFolder(path, tableFolder + MetadataXMLUtils.XML_EXTENSION, column, folder)) {
      if (folder == null || folder.isEmpty()) {
        setError(M_561_3, "lobFolder must be set for column type " + type + " on "
          + MetadataXMLUtils.createPath(schemaFolder, tableFolder, name, column));
      } else {
        setError(M_561_3,
          "not found lobFolder(" + folder + ") required by " + MetadataXMLUtils.createPath(path, name, column));
      }
      return false;
    }
    return true;
  }

  private boolean HasReferenceToLobFolder(String path, String table, String column, String folder) {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {

      final ZipArchiveEntry tableEntry = zipFile.getEntry(MetadataXMLUtils.createPath(path, table));
      final InputStream inputStream = zipFile.getInputStream(tableEntry);

      Document document = MetadataXMLUtils.getDocument(inputStream);
      String xpathExpressionDatabase = "/ns:table/ns:row/ns:" + column;

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath, MetadataXMLUtils.TABLE);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionDatabase);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          Element columnNumber = (Element) nodes.item(i);
          String fileName = columnNumber.getAttribute("file");

          if (!fileName.isEmpty() && zipFile.getEntry(MetadataXMLUtils.createPath(path, folder, fileName)) == null) {
            return false;
          }
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
   * M_5.6-1-1 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean noticeTypeOriginalUsed() {
    getValidationReporter().validationStatus(M_561_5, ValidationReporter.Status.OK, typeOriginalSet.toString());
    return true;
  }

  /**
   * M_5.6-1-12 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnDescription(String schema, String table, String description) {
    return validateXMLField(M_561_12, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema, Constants.TABLE, table);
  }
}
