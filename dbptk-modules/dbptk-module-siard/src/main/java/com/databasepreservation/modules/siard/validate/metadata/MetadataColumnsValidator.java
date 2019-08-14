package com.databasepreservation.modules.siard.validate.metadata;

import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.BINARY_LARGE_OBJECT;
import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.CHARACTER_LARGE_OBJECT;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private List<Element> columnsList = new ArrayList<>();
  private List<String> nameList = new ArrayList<>();
  private List<Map<String, String>> lobFolderList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();
  private Set<String> typeOriginalSet = new HashSet<>();

  public static ValidatorModule newInstance() {
    return new MetadataColumnsValidator();
  }

  private MetadataColumnsValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    if (!reportValidations(readXMLMetadataColumnLevel(), M_561, true)) {
      return false;
    }

    if (!reportValidations(validateColumnName(), M_561_1, true)) {
      return false;
    }

    if (!reportValidations(validateColumnLobFolder(), M_561_3, true)) {
      return false;
    }

    getValidationReporter().validationStatus(M_561_5, ValidationReporter.Status.OK, typeOriginalSet.toString());

    if (!reportValidations(validateColumnDescription(), M_561_12, true)) {
      return false;
    }

    return false;
  }

  private boolean readXMLMetadataColumnLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);
      for (int i = 0; i < nodes.getLength(); i++) {
        Element table = (Element) nodes.item(i);
        String tableFolderName = MetadataXMLUtils.getChildTextContext(table, "folder");

        Element schemaElement = (Element) table.getParentNode().getParentNode();
        String schemaFolderName = MetadataXMLUtils.getChildTextContext(schemaElement, "folder");

        Element columns = ((Element) table.getElementsByTagName("columns").item(0));
        NodeList columnNodes = columns.getElementsByTagName("column");

        for (int j = 0; j < columnNodes.getLength(); j++) {
          Element column = (Element) columnNodes.item(j);

          columnsList.add(column);

          // * M_5.6-1 The column name in SIARD is mandatory.
          String name = MetadataXMLUtils.getChildTextContext(column, "name");
          if (name == null || name.isEmpty()) {
            hasErrors = "Column name cannot be null schema: " + schemaFolderName + " table: " + tableFolderName;
            return false;
          }
          nameList.add(name);

          // * M_5.6-1 The column type in SIARD is mandatory.
          String type = MetadataXMLUtils.getChildTextContext(column, "type");
          if (type == null || type.isEmpty()) {
            String typeName = MetadataXMLUtils.getChildTextContext(column, "typeName");
            if (typeName == null || typeName.isEmpty()) {
              hasErrors = "Column type cannot be null schema: " + schemaFolderName + " table: " + tableFolderName
                + " column: " + name;
              return false;
            }
            // TODO check typeName in SIARD types node
            type = typeName;
          }

          if (type.equals(CHARACTER_LARGE_OBJECT) || type.equals(BINARY_LARGE_OBJECT) || type.equals(BLOB)
            || type.equals(CLOB) || type.equals(XML)) {

            Map<String, String> lobFolderMap = new HashMap<>();
            lobFolderMap.put("folder", MetadataXMLUtils.getChildTextContext(column, "lobFolder"));
            lobFolderMap.put("type", type);
            lobFolderMap.put("schema", schemaFolderName);
            lobFolderMap.put("table", tableFolderName);
            lobFolderMap.put("column", "c" + (j + 1));
            lobFolderMap.put("name", name);

            lobFolderList.add(lobFolderMap);
          }

          typeOriginalSet.add(MetadataXMLUtils.getChildTextContext(column, "typeOriginal"));
          descriptionList.add(MetadataXMLUtils.getChildTextContext(column, "description"));

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
  private boolean validateColumnName() {
    return validateMandatoryXMLFieldList(nameList, "name", false);
  }

  /**
   * M_5.6-1-3 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnLobFolder() {
    for (Map<String, String> lobFolder : lobFolderList) {
      String type = lobFolder.get("type");
      String folder = lobFolder.get("folder");
      String schema = lobFolder.get("schema");
      String table = lobFolder.get("table");
      String column = lobFolder.get("column");
      String name = lobFolder.get("name");

      String path = MetadataXMLUtils.createPath(MetadataXMLUtils.SIARD_CONTENT, schema, table);
      if (!HasReferenceToLobFolder(path, table + MetadataXMLUtils.XML_EXTENSION, column, folder)) {
        if (folder == null || folder.isEmpty()) {
          hasErrors = "lobFolder must be set for column type " + type + " on "
            + MetadataXMLUtils.createPath(path, name, column);
        } else {
          hasErrors = "not found lobFolder(" + folder + ") required by "
            + MetadataXMLUtils.createPath(path, name, column);
        }
        return false;
      }
    }
    return true;
  }

  /**
   * M_5.6-1-12 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnDescription() {
    validateXMLFieldSizeList(descriptionList, "description");
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
}
