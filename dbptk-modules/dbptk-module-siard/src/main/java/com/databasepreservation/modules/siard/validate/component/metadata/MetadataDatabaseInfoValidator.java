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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataDatabaseInfoValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataDatabaseInfoValidator.class);
  private final String MODULE_NAME;
  private static final String M_51 = "5.1";
  private static final String M_511 = "M_5.1-1";
  private static final String M_511_1 = "M_5.1-1-1";
  private static final String A_M_511_1 = "A_M_5.1-1-1";
  private static final String M_511_2 = "M_5.1-1-2";
  private static final String A_M_511_3 = "A_M_5.1-1-3";
  private static final String A_M_511_4 = "A_M_5.1-1-4";
  private static final String A_M_511_5 = "A_M_5.1-1-5";
  private static final String M_511_6 = "M_5.1-1-6";
  private static final String A_M_511_7 = "A_M_5.1-1-7";
  private static final String M_511_10 = "M_5.1-1-10";
  private static final String A_M_511_10 = "A_M_5.1-1-10";
  private static final String M_511_16 = "M_5.1-1-16";
  private static final String M_511_17 = "M_5.1-1-17";

  public MetadataDatabaseInfoValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_511, M_511_1, A_M_511_1, M_511_2, A_M_511_3, A_M_511_5, M_511_6, A_M_511_7, M_511_10,
      A_M_511_10, M_511_16, M_511_17);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_51);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_51, MODULE_NAME);

    validateMandatoryXSDFields(M_511, SIARD_ARCHIVE, "/ns:siardArchive");

    if (!readXMLMetadataDatabaseLevel()) {
      reportValidations(M_511, MODULE_NAME);
      reportValidations(M_511_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataDatabaseLevel() {
    if (preValidationRequirements())
      return false;
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      String version = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/@version", XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);
      // String version = nodes.item(0).getAttributes().item(2).getTextContent();
      validateSIARDVersion(version);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element database = (Element) nodes.item(i);
        String dbName = XMLUtils.getChildTextContext(database, Constants.DB_NAME);
        String path = buildPath(Constants.DB_NAME, dbName);
        validateDatabaseName(dbName, path);

        String description = XMLUtils.getChildTextContext(database, Constants.DESCRIPTION);
        validateDescription(description, path);

        String archiver = XMLUtils.getChildTextContext(database, Constants.ARCHIVER);
        validateArchiver(archiver, path);

        String archiverContact = XMLUtils.getChildTextContext(database, Constants.ARCHIVER_CONTACT);
        validateArchiverContact(archiverContact, path);

        String dataOwner = XMLUtils.getChildTextContext(database, Constants.DATA_OWNER);
        validateDataOwner(dataOwner, path);

        String dataOriginTimespan = XMLUtils.getChildTextContext(database, Constants.DATA_ORIGIN_TIMESPAN);
        validateDataOriginTimespan(dataOriginTimespan, path);

        String archivalDate = XMLUtils.getChildTextContext(database, Constants.ARCHIVAL_DATE);
        validateArchivalDate(archivalDate, path);

        NodeList schemasNodes = database.getElementsByTagName(Constants.SCHEMA);
        List<Element> schemasList = new ArrayList<>();
        for (int j = 0; j < schemasNodes.getLength(); j++) {
          schemasList.add((Element) schemasNodes.item(j));
        }
        validateSchemas(schemasList, path);

        NodeList usersNodes = database.getElementsByTagName(Constants.USER);
        List<Element> usersList = new ArrayList<>();
        for (int j = 0; j < usersNodes.getLength(); j++) {
          usersList.add((Element) usersNodes.item(j));
        }
        validateUsers(usersList, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read database info from SIARD file";
      setError(M_511, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.1-1-1: Version is mandatory in SIARD 2.1 specification
   *
   * A_M_511_1: Additional check, Version can be dk, 1.0, 2.0, 2.1., WARNING if it
   * is invalid
   *
   */
  private void validateSIARDVersion(String version) {
    // M_5.1-1-1
    validateXMLField(M_511_1, version, Constants.DB_NAME, true, true, "tag:siardArchive attribute:version");

    // A_M_511_1
    switch (version) {
      case "2.0":
      case "2.1":
      case "DK":
      case "1.0":
        break;
      default:
        addWarning(A_M_511_1, "The version of SIARD should be 1.0, DK, 2.0 or 2.1. Found: " + version, "siardArchive");
    }
  }

  /**
   * M_5.1-1-2 The database name is mandatory in SIARD 2.1 specification
   */
  private void validateDatabaseName(String dbName, String path) {
    validateXMLField(M_511_2, dbName, Constants.DB_NAME, true, true, path);
  }

  /**
   * A_M_5.1-1-3 The description field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is less than 3 characters
   */
  private void validateDescription(String description, String path) {
    validateXMLField(A_M_511_3, description, Constants.DESCRIPTION, true, true, path);
  }

  /**
   * A_M_5.1-1-4 The Archiver field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   */
  private void validateArchiver(String archiver, String path) {
    validateXMLField(A_M_511_4, archiver, Constants.ARCHIVER, true, true, path);
  }

  /**
   * M_5.1-1-5 The ArchiverContact field is mandatory in SIARD 2.1 specification
   */
  private void validateArchiverContact(String archiverContact, String path) {
    validateXMLField(A_M_511_5, archiverContact, Constants.ARCHIVER_CONTACT, true, true, path);
  }

  /**
   * M_5.1-1-6 The dataOwner field is mandatory in SIARD 2.1 specification
   */
  private void validateDataOwner(String dataOwner, String path) {
    validateXMLField(M_511_6, dataOwner, Constants.DATA_OWNER, true, true, path);
  }

  /**
   * A_M_5.1-1-7 The dataOriginTimespan field in SIARD file must not be empty.
   * ERROR when it is empty, WARNING if it is less than 3 characters
   */
  private void validateDataOriginTimespan(String dataOriginTimespan, String path) {
    validateXMLField(A_M_511_7, dataOriginTimespan, Constants.DATA_ORIGIN_TIMESPAN, true, true, path);
  }

  /**
   * M_5.1-1-10 The archivalDate field is mandatory in SIARD 2.1 specification
   * 
   * A_M_5.1-1-10 The archivalDate field in SIARD file should be a valid date.
   * WARNING if it is not valid date
   */
  private void validateArchivalDate(String archivalDate, String path) {
    if (archivalDate == null || archivalDate.isEmpty()) {
      String errorMessage = String.format("The archival date is mandatory (%s)", path);
      setError(M_511_10, errorMessage);
      setError(A_M_511_10, errorMessage);
      return;
    }
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
      .withChronology(GJChronology.getInstanceUTC());

    try {
      formatter.parseDateTime(archivalDate);
    } catch (UnsupportedOperationException | IllegalArgumentException e) {
      String warningMessage = String.format("The archival date '%s' is not a valid date", archivalDate);
      addWarning(A_M_511_10, warningMessage, path);
    }
  }

  /**
   * M_5.1-1-16 The schemas field in SIARD file must not be empty. ERROR when it
   * is empty
   */
  private void validateSchemas(List<Element> schemasList, String path) {
    if (schemasList == null || schemasList.isEmpty()) {
      String errorMessage = String.format("Schema node is mandatory (%s)", path);
      setError(M_511_16, errorMessage);
    }
  }

  /**
   * M_5.1-1-17 The users field in SIARD file must not be empty. ERROR when it is
   * empty
   */
  private void validateUsers(List<Element> usersList, String path) {
    if (usersList == null || usersList.isEmpty()) {
      String errorMessage = String.format("User node is mandatory (%s)", path);
      setError(M_511_17, errorMessage);
    }
  }
}
