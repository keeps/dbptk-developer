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

  private List<Element> tableList = new ArrayList<>();
  private List<String> nameList = new ArrayList<>();
  private List<String> folderList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();
  private List<String> columnsList = new ArrayList<>();
  private List<String> rowsList = new ArrayList<>();

  public static MetadataTableValidator newInstance() {
    return new MetadataTableValidator();
  }

  private MetadataTableValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_55, MODULE_NAME);
    if (!reportValidations(readXMLMetadataTable(), M_551, true)) {
      return false;
    }

    if (!reportValidations(validateTableName(), M_551_1, true)) {
      return false;
    }

    if (!reportValidations(validateTableFolder(), M_551_2, true)) {
      return false;
    }

    if (!reportValidations(validateTableDescription(), M_551_3, true)) {
      return false;
    }

    if (!reportValidations(validateTableColumns(), M_551_4, true)) {
      return false;
    }

    if (!reportValidations(validateTableRows(), M_551_10, true)) {
      return false;
    }

    return true;
  }

  private boolean readXMLMetadataTable() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final ZipArchiveEntry metadataEntry = zipFile.getEntry("header/metadata.xml");
      final InputStream inputStream = zipFile.getInputStream(metadataEntry);
      Document document = MetadataXMLUtils.getDocument(inputStream);
      String xpathExpressionDatabase = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table";

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath, null);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionDatabase);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          Element table = (Element) nodes.item(i);
          tableList.add(table);

          Element nameElement = MetadataXMLUtils.getChild(table, "name");
          String name = nameElement != null ? nameElement.getTextContent() : null;
          nameList.add(name);

          Element folderElement = MetadataXMLUtils.getChild(table, "folder");
          String folder = folderElement != null ? folderElement.getTextContent() : null;
          folderList.add(folder);

          Element descriptionElement = MetadataXMLUtils.getChild(table, "description");
          String description = descriptionElement != null ? descriptionElement.getTextContent() : null;
          descriptionList.add(description);

          Element columnsElement = MetadataXMLUtils.getChild(table, "columns");
          String columns = columnsElement != null ? columnsElement.getTextContent() : null;
          columnsList.add(columns);

          Element rowsElement = MetadataXMLUtils.getChild(table, "rows");
          String rows = rowsElement != null ? rowsElement.getTextContent() : null;
          rowsList.add(rows);

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
   * M_5.5-1-1 The table name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableName() {
    return validateMandatoryXMLFieldList(nameList, "name", true);
  }

  /**
   * M_5.5-1-2 The table folder in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableFolder() {
    return validateMandatoryXMLFieldList(folderList, "folder", false);
  }

  /**
   * M_5.5-1-3 The table description in SIARD file must not be less than 3 characters.
   *
   */
  private boolean validateTableDescription() {
    validateXMLFieldSizeList(descriptionList, "description");
    return true;
  }

  /**
   * M_5.5-1-4 The table columns in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableColumns() {
    return validateMandatoryXMLFieldList(columnsList, "columns", false);
  }

  /**
   * M_5.5-1-10 The table rows in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateTableRows() {
    return validateMandatoryXMLFieldList(rowsList, "rows", false);
  }
}
