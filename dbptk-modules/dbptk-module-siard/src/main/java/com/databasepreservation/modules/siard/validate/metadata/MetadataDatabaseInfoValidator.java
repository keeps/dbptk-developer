package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataDatabaseInfoValidator extends MetadataValidator {
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

  private static final String SIARDFILE = "SIARD file";
  private static final String DB_NAME = "dbname";
  private static final String DB_ARCHIVER = "archiver";
  private static final String DB_ARCHIVER_CONTACT = "archiverContact";
  private static final String DB_DATA_OWNER = "dataOwner";
  private static final String DB_DATA_ORIGIN_TIMESPAN = "dataOriginTimespan";
  private static final String DB_ARCHIVAL_DATE = "archivalDate";

  public static MetadataDatabaseInfoValidator newInstance() {
    return new MetadataDatabaseInfoValidator();
  }

  private MetadataDatabaseInfoValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_51, MODULE_NAME);
    readXMLMetadataDatabaseLevel();

    return reportValidations(M_511) && reportValidations(M_511_1) && reportValidations(M_511_2)
      && reportValidations(M_511_3) && reportValidations(M_511_4) && reportValidations(M_511_5)
      && reportValidations(M_511_6) && reportValidations(M_511_7) && reportValidations(M_511_10)
      && reportValidations(M_511_16) && reportValidations(M_511_17);
  }

  private boolean readXMLMetadataDatabaseLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      String version = nodes.item(0).getAttributes().item(2).getTextContent();
      validateSIARDVersion(version);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element database = (Element) nodes.item(i);

        String dbName = MetadataXMLUtils.getChildTextContext(database, DB_NAME);
        String description = MetadataXMLUtils.getChildTextContext(database, Constants.DESCRIPTION);
        String archiver = MetadataXMLUtils.getChildTextContext(database, DB_ARCHIVER);
        String archiverContact = MetadataXMLUtils.getChildTextContext(database, DB_ARCHIVER_CONTACT);
        String dataOwner = MetadataXMLUtils.getChildTextContext(database, DB_DATA_OWNER);
        String dataOriginTimespan = MetadataXMLUtils.getChildTextContext(database, DB_DATA_ORIGIN_TIMESPAN);
        String archivalDate = MetadataXMLUtils.getChildTextContext(database, DB_ARCHIVAL_DATE);

        NodeList schemasNodes = database.getElementsByTagName(Constants.SCHEMA);
        List<Element> schemasList = new ArrayList<>();
        for (int j = 0; j < schemasNodes.getLength(); j++) {
          schemasList.add((Element) schemasNodes.item(j));
        }

        NodeList usersNodes = database.getElementsByTagName(Constants.USER);
        List<Element> usersList = new ArrayList<>();
        for (int j = 0; j < usersNodes.getLength(); j++) {
          usersList.add((Element) usersNodes.item(j));
        }

        if (!validateDatabaseName(dbName) || !validateDescription(dbName, description)
          || !validateArchiver(dbName, archiver) || !validateArchiverContact(dbName, archiverContact)
          || !validateDataOwner(dbName, dataOwner) || !validateDataOriginTimespan(dbName, dataOriginTimespan)
          || !validateArchivalDate(dbName, archivalDate) || !validateSchemas(dbName, schemasList)
          || !validateUsers(dbName, usersList)) {
          break;
        }

      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.1-1-3 The database name in SIARD file must not be empty. ERROR when it is
   * empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateSIARDVersion(String version) {
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
          addWarning(M_511_1, "The version of SIARD file is" + version);
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
  private boolean validateDatabaseName(String dbName) {
    return validateXMLField(M_511_2, dbName, DB_NAME, true, true, SIARDFILE);
  }

  /**
   * M_5.1-1-3 The description field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is less than 3 characters
   *
   */
  private boolean validateDescription(String dbName, String description) {
    return validateXMLField(M_511_3, description, Constants.DESCRIPTION, true, true, DB_NAME, dbName);
  }

  /**
   * M_5.1-1-4 The Archiver field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiver(String dbName, String archiver) {
    return validateXMLField(M_511_4, archiver, DB_ARCHIVER, true, true, DB_NAME, dbName);
  }

  /**
   * M_5.1-1-5 The ArchiverContact field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiverContact(String dbName, String archiverContact) {
    return validateXMLField(M_511_5, archiverContact, DB_ARCHIVER_CONTACT, true, true, DB_NAME, dbName);
  }

  /**
   * M_5.1-1-6 The dataOwner field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOwner(String dbName, String dataOwner) {
    return validateXMLField(M_511_6, dataOwner, DB_DATA_OWNER, true, true, DB_NAME, dbName);
  }

  /**
   * M_5.1-1-7 The dataOriginTimespan field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOriginTimespan(String dbName, String dataOriginTimespan) {
    return validateXMLField(M_511_7, dataOriginTimespan, DB_DATA_ORIGIN_TIMESPAN, true, true, DB_NAME, dbName);
  }

  /**
   * M_5.1-1-10 The archivalDate field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is not valid date
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchivalDate(String dbName, String archivalDate) {
    if (archivalDate == null || archivalDate.isEmpty()) {
      String errorMessage = String.format("The archival date inside SIARD file of database '%s' file is mandatory",
        dbName);
      setError(M_511_10, errorMessage);
      return false;
    }
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
      .withChronology(GJChronology.getInstanceUTC());

    try {
      formatter.parseDateTime(archivalDate);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      String warningMessage = String
        .format("The archival date '%s' inside SIARD file of database '%s' is not a valid date", archivalDate, dbName);
      addWarning(M_511_10, warningMessage);
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
  private boolean validateSchemas(String dbName, List<Element> schemasList) {
    if (schemasList == null || schemasList.isEmpty()) {
      String errorMessage = String.format("The schema inside SIARD of database '%s' file is mandatory", dbName);
      setError(M_511_16, errorMessage);
      return false;
    }
    return true;
  }

  /**
   * M_5.1-1-17 The users field in SIARD file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateUsers(String dbName, List<Element> usersList) {
    if (usersList == null || usersList.isEmpty()) {
      String errorMessage = String.format("The users inside SIARD of database '%s' file is mandatory", dbName);
      setError(M_511_17, errorMessage);
      return false;
    }
    return true;
  }
}
