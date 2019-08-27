package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.utils.XMLUtils;

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

  public static MetadataDatabaseInfoValidator newInstance() {
    return new MetadataDatabaseInfoValidator();
  }

  private MetadataDatabaseInfoValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_51, MODULE_NAME);

    if (!readXMLMetadataDatabaseLevel()) {
      reportValidations(M_511, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_511, MODULE_NAME) && reportValidations(M_511_1, MODULE_NAME)
      && reportValidations(M_511_2, MODULE_NAME) && reportValidations(M_511_3, MODULE_NAME)
      && reportValidations(M_511_4, MODULE_NAME) && reportValidations(M_511_5, MODULE_NAME)
      && reportValidations(M_511_6, MODULE_NAME) && reportValidations(M_511_7, MODULE_NAME)
      && reportValidations(M_511_10, MODULE_NAME) && reportValidations(M_511_16, MODULE_NAME)
      && reportValidations(M_511_17, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;

  }

  private boolean readXMLMetadataDatabaseLevel() {
    if (preValidationRequirements())
      return false;
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      String version = nodes.item(0).getAttributes().item(2).getTextContent();
      validateSIARDVersion(version);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element database = (Element) nodes.item(i);

        String dbName = XMLUtils.getChildTextContext(database, Constants.DB_NAME);
        String description = XMLUtils.getChildTextContext(database, Constants.DESCRIPTION);
        String archiver = XMLUtils.getChildTextContext(database, Constants.ARCHIVER);
        String archiverContact = XMLUtils.getChildTextContext(database, Constants.ARCHIVER_CONTACT);
        String dataOwner = XMLUtils.getChildTextContext(database, Constants.DATA_OWNER);
        String dataOriginTimespan = XMLUtils.getChildTextContext(database, Constants.DATA_ORIGIN_TIMESPAN);
        String archivalDate = XMLUtils.getChildTextContext(database, Constants.ARCHIVAL_DATE);

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

        String path = buildPath(Constants.DB_NAME, dbName);

        if (!validateDatabaseName(dbName) || !validateDescription(description, path)
          || !validateArchiver(archiver, path) || !validateArchiverContact(archiverContact, path)
          || !validateDataOwner(dataOwner, path) || !validateDataOriginTimespan(dataOriginTimespan, path)
          || !validateArchivalDate(archivalDate, path) || !validateSchemas(schemasList, path)
          || !validateUsers(usersList, path)) {
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
          addWarning(M_511_1, "The version of SIARD file is" + version, "SIARD file");
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
    return validateXMLField(M_511_2, dbName, Constants.DB_NAME, true, true);
  }

  /**
   * M_5.1-1-3 The description field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is less than 3 characters
   *
   */
  private boolean validateDescription(String description, String path) {
    return validateXMLField(M_511_3, description, Constants.DESCRIPTION, true, true, path);
  }

  /**
   * M_5.1-1-4 The Archiver field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiver(String archiver, String path) {
    return validateXMLField(M_511_4, archiver, Constants.ARCHIVER, true, true, path);
  }

  /**
   * M_5.1-1-5 The ArchiverContact field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchiverContact(String archiverContact, String path) {
    return validateXMLField(M_511_5, archiverContact, Constants.ARCHIVER_CONTACT, true, true, path);
  }

  /**
   * M_5.1-1-6 The dataOwner field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOwner(String dataOwner, String path) {
    return validateXMLField(M_511_6, dataOwner, Constants.DATA_OWNER, true, true, path);
  }

  /**
   * M_5.1-1-7 The dataOriginTimespan field in SIARD file must not be empty. ERROR
   * when it is empty, WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateDataOriginTimespan(String dataOriginTimespan, String path) {
    return validateXMLField(M_511_7, dataOriginTimespan, Constants.DATA_ORIGIN_TIMESPAN, true, true, path);
  }

  /**
   * M_5.1-1-10 The archivalDate field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is not valid date
   *
   * @return true if valid otherwise false
   */
  private boolean validateArchivalDate(String archivalDate, String path) {
    if (archivalDate == null || archivalDate.isEmpty()) {
      String errorMessage = String.format("The archival date is mandatory (%s)", path);
      setError(M_511_10, errorMessage);
      return false;
    }
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
      .withChronology(GJChronology.getInstanceUTC());

    try {
      formatter.parseDateTime(archivalDate);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      String warningMessage = String.format("The archival date '%s' is not a valid date", archivalDate);
      addWarning(M_511_10, warningMessage, path);
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
  private boolean validateSchemas(List<Element> schemasList, String path) {
    if (schemasList == null || schemasList.isEmpty()) {
      String errorMessage = String.format("Schema node is mandatory (%s)", path);
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
  private boolean validateUsers(List<Element> usersList, String path) {
    if (usersList == null || usersList.isEmpty()) {
      String errorMessage = String.format("User node is mandatory (%s)", path);
      setError(M_511_17, errorMessage);
      return false;
    }
    return true;
  }
}
