/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
import com.databasepreservation.model.reporters.ValidationReporterStatus;
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
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_51);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_51, MODULE_NAME);

    NodeList nodes;
    String version;
    try {
      nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      version = (String) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/@version", XPathConstants.STRING, Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read database info from SIARD file";
      setError(M_511, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    validateMandatoryXSDFields(M_511, SIARD_ARCHIVE, "/ns:siardArchive");

    if (validateSIARDVersion(version)) {
      validationOk(MODULE_NAME, M_511_1);
      validationOk(MODULE_NAME, A_M_511_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_1, ValidationReporterStatus.ERROR);
      observer.notifyValidationStep(MODULE_NAME, A_M_511_1, ValidationReporterStatus.ERROR);
    }

    if (validateDatabaseName(nodes)) {
      validationOk(MODULE_NAME, M_511_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_2, ValidationReporterStatus.ERROR);
    }

    if (validateDescription(nodes)) {
      validationOk(MODULE_NAME, A_M_511_3);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_3, ValidationReporterStatus.ERROR);
    }

    if (validateArchiver(nodes)) {
      validationOk(MODULE_NAME, A_M_511_4);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_4, ValidationReporterStatus.ERROR);
    }

    if (validateArchiverContact(nodes)) {
      validationOk(MODULE_NAME, A_M_511_5);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_5, ValidationReporterStatus.ERROR);
    }

    if (validateDataOwner(nodes)) {
      validationOk(MODULE_NAME, M_511_6);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_6, ValidationReporterStatus.ERROR);
    }

    if (validateDataOriginTimespan(nodes)) {
      validationOk(MODULE_NAME, A_M_511_7);
    } else {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_7, ValidationReporterStatus.ERROR);
    }

    if (validateArchivalDate(nodes)) {
      validationOk(MODULE_NAME, M_511_10);
      validationOk(MODULE_NAME, A_M_511_10);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_10, ValidationReporterStatus.ERROR);
      observer.notifyValidationStep(MODULE_NAME, A_M_511_10, ValidationReporterStatus.ERROR);
    }

    if (validateSchemas(nodes)) {
      validationOk(MODULE_NAME, M_511_16);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_16, ValidationReporterStatus.ERROR);
    }

    if (validateUsers(nodes)) {
      validationOk(MODULE_NAME, M_511_17);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_17, ValidationReporterStatus.ERROR);
    }

    closeZipFile();

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.1-1-1: Version is mandatory in SIARD 2.1 specification
   *
   * A_M_511_1: Additional check, Version can be dk, 1.0, 2.0, 2.1., WARNING if it
   * is invalid
   *
   */
  private boolean validateSIARDVersion(String version) {
    // M_5.1-1-1
    if (!validateXMLField(M_511_1, version, Constants.DB_NAME, true, true, "tag:siardArchive attribute:version")) {
      return false;
    }

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
    return true;
  }

  /**
   * M_5.1-1-2 The database name is mandatory in SIARD 2.1 specification
   */
  private boolean validateDatabaseName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String dbName = XMLUtils.getChildTextContext(database, Constants.DB_NAME);
      String path = buildPath(Constants.DB_NAME, dbName);
      if (!validateXMLField(M_511_2, dbName, Constants.DB_NAME, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * A_M_5.1-1-3 The description field in SIARD file must not be empty. ERROR when
   * it is empty, WARNING if it is less than 3 characters
   */
  private boolean validateDescription(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String description = XMLUtils.getChildTextContext(database, Constants.DESCRIPTION);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      if (!validateXMLField(A_M_511_3, description, Constants.DESCRIPTION, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * A_M_5.1-1-4 The Archiver field in SIARD file must not be empty. ERROR when it
   * is empty, WARNING if it is less than 3 characters
   */
  private boolean validateArchiver(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String archiver = XMLUtils.getChildTextContext(database, Constants.ARCHIVER);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      if (!validateXMLField(A_M_511_4, archiver, Constants.ARCHIVER, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.1-1-5 The ArchiverContact field is mandatory in SIARD 2.1 specification
   */
  private boolean validateArchiverContact(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String archiverContact = XMLUtils.getChildTextContext(database, Constants.ARCHIVER_CONTACT);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      if (!validateXMLField(A_M_511_5, archiverContact, Constants.ARCHIVER_CONTACT, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.1-1-6 The dataOwner field is mandatory in SIARD 2.1 specification
   */
  private boolean validateDataOwner(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String dataOwner = XMLUtils.getChildTextContext(database, Constants.DATA_OWNER);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      if (!validateXMLField(M_511_6, dataOwner, Constants.DATA_OWNER, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * A_M_5.1-1-7 The dataOriginTimespan field in SIARD file must not be empty.
   * ERROR when it is empty, WARNING if it is less than 3 characters
   */
  private boolean validateDataOriginTimespan(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String dataOriginTimespan = XMLUtils.getChildTextContext(database, Constants.DATA_ORIGIN_TIMESPAN);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      if (!validateXMLField(A_M_511_7, dataOriginTimespan, Constants.DATA_ORIGIN_TIMESPAN, true, true, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.1-1-10 The archivalDate field is mandatory in SIARD 2.1 specification
   * 
   * A_M_5.1-1-10 The archivalDate field in SIARD file should be a valid date.
   * WARNING if it is not valid date
   */
  private boolean validateArchivalDate(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      String archivalDate = XMLUtils.getChildTextContext(database, Constants.ARCHIVAL_DATE);

      if (archivalDate == null || archivalDate.isEmpty()) {
        String errorMessage = String.format("The archival date is mandatory (%s)", path);
        setError(M_511_10, errorMessage);
        setError(A_M_511_10, errorMessage);
        hasErrors = true;
        continue;
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
    return !hasErrors;
  }

  /**
   * M_5.1-1-16 The schemas field in SIARD file must not be empty. ERROR when it
   * is empty
   */
  private boolean validateSchemas(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      NodeList schemasNodes = database.getElementsByTagName(Constants.SCHEMA);

      List<Element> schemasList = new ArrayList<>();
      for (int j = 0; j < schemasNodes.getLength(); j++) {
        schemasList.add((Element) schemasNodes.item(j));
      }

      if (schemasList == null || schemasList.isEmpty()) {
        String errorMessage = String.format("Schema node is mandatory (%s)", path);
        setError(M_511_16, errorMessage);
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.1-1-17 The users field in SIARD file must not be empty. ERROR when it is
   * empty
   */
  private boolean validateUsers(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element database = (Element) nodes.item(i);
      String path = buildPath(Constants.DB_NAME, XMLUtils.getChildTextContext(database, Constants.DB_NAME));
      NodeList usersNodes = database.getElementsByTagName(Constants.USER);

      List<Element> usersList = new ArrayList<>();
      for (int j = 0; j < usersNodes.getLength(); j++) {
        usersList.add((Element) usersNodes.item(j));
      }

      if (usersList == null || usersList.isEmpty()) {
        String errorMessage = String.format("User node is mandatory (%s)", path);
        setError(M_511_17, errorMessage);
        hasErrors = true;
      }
    }
    return !hasErrors;
  }
}
