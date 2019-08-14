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

import com.databasepreservation.model.reporters.ValidationReporter;

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

  private List<String> names = new ArrayList<>();
  private List<String> folders = new ArrayList<>();
  private List<String> descriptions = new ArrayList<>();
  private List<String> tablesList = new ArrayList<>();

  public static MetadataSchemaValidator newInstance() {
    return new MetadataSchemaValidator();
  }

  private MetadataSchemaValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_52, MODULE_NAME);

    if (!validateMandatorySchemaMetadata()) {
      getValidationReporter().validationStatus(M_521, ValidationReporter.Status.ERROR);
      return false;
    }

    getValidationReporter().validationStatus(M_521, ValidationReporter.Status.OK);

    // Validade Schemas Name
    if (!validateSchemaName()) {
      getValidationReporter().validationStatus(M_521_1, ValidationReporter.Status.ERROR);
      return false;
    } else if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(M_521_1, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(M_521_1, ValidationReporter.Status.OK);
    }

    // Validade Schemas Folder
    if (!validateSchemaFolder()) {
      getValidationReporter().validationStatus(M_521_2, ValidationReporter.Status.ERROR);
      return false;
    } else if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(M_521_2, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(M_521_2, ValidationReporter.Status.OK);
    }

    // Validade Schemas Descriptions
    validateSchemaDescription();
    if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(M_521_4, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(M_521_4, ValidationReporter.Status.OK);
    }

    // Validade Schemas Tables
    if (!validateSchemaTable()) {
      getValidationReporter().validationStatus(M_521_5, ValidationReporter.Status.ERROR);
      return false;
    } else {
      getValidationReporter().validationStatus(M_521_5, ValidationReporter.Status.OK);
    }

    return true;
  }

  /**
   * M_5.2-1 All metadata that are designated as mandatory in metadata.xsd at
   * schema level must be completed accordingly.
   *
   * @return true if valid otherwise false
   */
  private boolean validateMandatorySchemaMetadata() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final ZipArchiveEntry metadataEntry = zipFile.getEntry("header/metadata.xml");
      final InputStream inputStream = zipFile.getInputStream(metadataEntry);
      Document document = MetadataXMLUtils.getDocument(inputStream);

      String xpathExpressionSchemas = "/ns:siardArchive/ns:schemas/ns:schema";

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath, null);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionSchemas);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          Element schema = (Element) nodes.item(i);

          Element nameElement = MetadataXMLUtils.getChild(schema, "name");
          String name = nameElement != null ? nameElement.getTextContent() : null;

          Element folderElement = MetadataXMLUtils.getChild(schema, "folder");
          String folder = folderElement != null ? folderElement.getTextContent() : null;

          Element tablesElement = MetadataXMLUtils.getChild(schema, "tables");
          String tables = tablesElement != null ? tablesElement.getTextContent() : null;

          Element descriptionElement = MetadataXMLUtils.getChild(schema, "description");
          String description = descriptionElement != null ? descriptionElement.getTextContent() : null;

          // M_5.2-1
          if ((name == null || name.isEmpty()) || (folder == null || folder.isEmpty())) {
            return false;
          }

          names.add(name);
          folders.add(folder);
          descriptions.add(description);
          tablesList.add(tables);
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
   * M_5.2-1-1 The schema name in the database must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaName() {
    hasWarnings.clear();
    for (String name : names) {
      if (!validateMandatoryXMLField(name, "name", true)) {
        return false;
      }
    }
    return true;
  }

  /**
   * M_5.2-1-2 The schema folder in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaFolder() {
    hasWarnings.clear();
    for (String folder : folders) {
      if (!validateMandatoryXMLField(folder, "folder", true)) {
        return false;
      }
    }
    return true;
  }

  /**
   * M_5.2-1-4 if schema description in the database is less than 3 characters a
   * warning must be send
   *
   * @return true if valid otherwise false
   */
  private void validateSchemaDescription() {
    hasWarnings.clear();
    for (String description : descriptions) {
      validateXMLFieldSize(description, "description");
    }
  }

  /**
   * M_5.2-1-5 The schema tables in the database must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemaTable() {
    for (String table : tablesList) {
      if (table == null || table.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
