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
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataDatabaseInfoValidator extends ValidatorModule {
  private static final String MODULE_NAME = "Database level metadata";
  private static final String M_51 = "5.1";
  private static final String M_511 = "M_5.1-1";
  private static final String M_511_1 = "M_5.1-1-1";
  private static final String M_511_2 = "M_5.1-1-2";
  private static final String M_511_3 = "M_5.1-1-3";
  private static final String M_511_4 = "M_5.1-1-4";
  private static final String M_511_5 = "M_5.1-1-5";
  private static final String M_511_6 = "M_5.1-1-6";
  private static final String M_511_7 = "M_5.1-1-7";
  private static final String M_511_10 = "M_5.1-1-10";
  private static final String M_511_11 = "M_5.1-1-11"; // TODO
  private static final String M_511_16 = "M_5.1-1-16";
  private static final String M_511_17 = "M_5.1-1-17";

  private List<String> hasWarnings = new ArrayList<>();

  private String version;
  private String dbName;
  private String description;
  private String archiver;
  private String archiverContact;
  private String dataOwner;
  private String dataOriginTimespan;
  private String archivalDate;
  private List<Element> schemasList = new ArrayList<>();
  private List<Element> usersList = new ArrayList<>();

  public static MetadataDatabaseInfoValidator newInstance() {
    return new MetadataDatabaseInfoValidator();
  }

  private MetadataDatabaseInfoValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_51, MODULE_NAME);

    if (!reportValidations(readXMLMetadataDatabaseLevel(), M_511, true)) {
      return false;
    }

    if (!reportValidations(validateSIARDVersion(), M_511_1, true)) {
      return false;
    }

    if (!reportValidations(validateDatabaseName(), M_511_2, true)) {
      return false;
    }

    if (!reportValidations(validateDescription(), M_511_3, false)) {
      return false;
    }

    if (!reportValidations(validateArchiver(), M_511_4, true)) {
      return false;
    }

    if (!reportValidations(validateArchiverContact(), M_511_5, true)) {
      return false;
    }

    if (!reportValidations(validateDataOwner(), M_511_6, true)) {
      return false;
    }

    if (!reportValidations(validateDataOriginTimespan(), M_511_7, true)) {
      return false;
    }

    if (!reportValidations(validateArchivalDate(), M_511_10, true)) {
      return false;
    }

    if (!reportValidations(validateSchemas(), M_511_16, true)) {
      return false;
    }

    if (!reportValidations(validateUsers(), M_511_17, true)) {
      return false;
    }

    return true;
  }

  private boolean readXMLMetadataDatabaseLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final ZipArchiveEntry metadataEntry = zipFile.getEntry("header/metadata.xml");
      final InputStream inputStream = zipFile.getInputStream(metadataEntry);
      Document document = MetadataXMLUtils.getDocument(inputStream);
      String xpathExpressionDatabase = "/ns:siardArchive";

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionDatabase);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        version = nodes.item(0).getAttributes().item(2).getTextContent();
        for (int i = 0; i < nodes.getLength(); i++) {
          Element database = (Element) nodes.item(i);

          Element dbNameElement = MetadataXMLUtils.getChild(database, "dbname");
          dbName = dbNameElement != null ? dbNameElement.getTextContent() : null;

          Element descriptionElement = MetadataXMLUtils.getChild(database, "description");
          description = descriptionElement != null ? descriptionElement.getTextContent() : null;

          Element archiverElement = MetadataXMLUtils.getChild(database, "archiver");
          archiver = archiverElement != null ? archiverElement.getTextContent() : null;

          Element archiverContactElement = MetadataXMLUtils.getChild(database, "archiverContact");
          archiverContact = archiverContactElement != null ? archiverContactElement.getTextContent() : null;

          Element dataOwnerElement = MetadataXMLUtils.getChild(database, "dataOwner");
          dataOwner = dataOwnerElement != null ? dataOwnerElement.getTextContent() : null;

          Element dataOriginTimespanElement = MetadataXMLUtils.getChild(database, "dataOriginTimespan");
          dataOriginTimespan = dataOriginTimespanElement != null ? dataOriginTimespanElement.getTextContent() : null;

          Element archivalDateElement = MetadataXMLUtils.getChild(database, "archivalDate");
          archivalDate = archivalDateElement != null ? archivalDateElement.getTextContent() : null;

          NodeList schemasNodes = database.getElementsByTagName("schema");
          for (int j = 0; j < schemasNodes.getLength(); j++) {
            schemasList.add((Element) schemasNodes.item(j));
          }

          NodeList usersNodes = database.getElementsByTagName("user");
          for (int j = 0; j < usersNodes.getLength(); j++) {
            usersList.add((Element) usersNodes.item(j));
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

  private boolean reportValidations(boolean result, String codeID, boolean mandatory) {
    if (!result && mandatory) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR);
      return false;
    } else if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
    return true;
  }

  /**
   * M_5.1-1-3 The database name in SIARD file must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSIARDVersion() {
    if (version == null || version.isEmpty()) {
      return false;
    } else {
      switch (version) {
        case "2.0":
        case "2.1":
        case "DK":
        case "1.0":
          break;
        default:
          hasWarnings.add("Version:" + version);
      }
    }
    return true;
  }

  /**
   * M_5.1-1-2 The database name in SIARD file must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDatabaseName() {
    return MetadataXMLUtils.validateMandatoryXMLField(dbName, "dbName", hasWarnings);
  }

  /**
   * M_5.1-1-3 The description field in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   *
   */
  private boolean validateDescription() {
    MetadataXMLUtils.validateXMLFieldSize(description, "description", hasWarnings);
    return true;
  }

  /**
   * M_5.1-1-4 The Archiver field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiver() {
    return MetadataXMLUtils.validateMandatoryXMLField(archiver, "archiver", hasWarnings);
  }

  /**
   * M_5.1-1-5 The ArchiverContact field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiverContact() {
    return MetadataXMLUtils.validateMandatoryXMLField(archiverContact, "archiverContact", hasWarnings);
  }

  /**
   * M_5.1-1-6 The dataOwner field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOwner() {
    return MetadataXMLUtils.validateMandatoryXMLField(dataOwner, "dataOwner", hasWarnings);
  }

  /**
   * M_5.1-1-7 The dataOriginTimespan field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOriginTimespan() {
    return MetadataXMLUtils.validateMandatoryXMLField(dataOriginTimespan, "dataOriginTimespan", hasWarnings);
  }

  /**
   * M_5.1-1-10 The archivalDate field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is not valid date
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchivalDate() {
    if (archivalDate == null || archivalDate.isEmpty()) {
      return false;
    }
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
      .withChronology(GJChronology.getInstanceUTC());

    try {
      formatter.parseDateTime(archivalDate);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.1-1-16 The schemas field in SIARD file must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateSchemas() {
    return schemasList != null && !schemasList.isEmpty();
  }

  /**
   * M_5.1-1-17 The users field in SIARD file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateUsers() {
    return usersList != null && !usersList.isEmpty();
  }
}
