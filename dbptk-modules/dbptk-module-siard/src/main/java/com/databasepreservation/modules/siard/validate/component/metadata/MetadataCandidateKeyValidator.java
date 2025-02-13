/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

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
public class MetadataCandidateKeyValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataCandidateKeyValidator.class);
  private final String MODULE_NAME;
  private static final String M_511 = "5.11";
  private static final String M_511_1 = "M_5.11-1";
  private static final String M_511_1_1 = "M_5.11-1-1";
  private static final String A_M_511_1_1 = "A_M_5.11-1-1";
  private static final String M_511_1_2 = "M_5.11-1-2";
  private static final String A_M_511_1_2 = "A_M_5.11-1-2";
  private static final String A_M_511_1_3 = "A_M_5.11-1-3";
  private static final String BLANK = " ";
  private boolean additionalCheckError = false;

  public MetadataCandidateKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_511);

    getValidationReporter().moduleValidatorHeader(M_511, MODULE_NAME);

    NodeList tables;
    NodeList keys;
    try (
      InputStream isTables = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath());
      InputStream isKeys = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      tables = (NodeList) XMLUtils.getXPathResult(isTables,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      keys = (NodeList) XMLUtils.getXPathResult(isKeys,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:candidateKeys/ns:candidateKey",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read primary key from SIARD file";
      setError(M_511_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (keys.getLength() == 0) {
      getValidationReporter().skipValidation(M_511_1, "Database has no Candidate keys");
      observer.notifyValidationStep(MODULE_NAME, M_511_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_511_1, UNIQUE_KEY_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:candidateKeys/ns:candidateKey");

    if (validateCandidateKeyName(keys)) {
      validationOk(MODULE_NAME, M_511_1_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_511_1_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_511_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        validationOk(MODULE_NAME, A_M_511_1_1);
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_1_1, ValidationReporterStatus.ERROR);
      observer.notifyValidationStep(MODULE_NAME, A_M_511_1_1, ValidationReporterStatus.ERROR);
    }

    if (validateCandidateKeyColumn(tables, keys)) {
      validationOk(MODULE_NAME, M_511_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_511_1_2, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_1_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_511_1_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_511_1_2);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_511_1_2, ValidationReporterStatus.ERROR);
      }
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_511_1_3, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_511_1_3, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateCandidateKeyDescription(keys);
      validationOk(MODULE_NAME, A_M_511_1_3);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.11-1-1 The Candidate Key name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.11-1-1 validation that candidate key name not contain any blanks
   */
  private boolean validateCandidateKeyName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element candidateKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(candidateKey, Constants.SCHEMA),
          Constants.TABLE, XMLUtils.getParentNameByTagName(candidateKey, Constants.TABLE), Constants.CANDIDATE_KEY,
          Integer.toString(i));
      // if (this.skipAdditionalChecks) {
      //  observer.notifyValidationStep(MODULE_NAME, A_P_4310, ValidationReporterStatus.SKIPPED);
      //  getValidationReporter().skipValidation(A_P_4310, ADDITIONAL_CHECKS_SKIP_REASON);
      // } else {
      String name = XMLUtils.getChildTextContext(candidateKey, Constants.NAME);

      boolean result = validateXMLField(M_511_1_1, name, Constants.CANDIDATE_KEY, true, false, path);
      if (!this.skipAdditionalChecks) {
        if (result) {
          if (name.contains(BLANK)) {
            addWarning(A_M_511_1_1, "Candidate key " + name + " contain blanks in name", path);
          }
          continue;
        }
        setError(A_M_511_1_1, String.format("Aborted because candidate key name is mandatory (%s)", path));
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * M_5.11-1-2 The Candidate Key column is mandatory in SIARD 2.1 specification
   *
   * M_5.11-1-2 The Candidate Key column in SIARD file must exist on table. ERROR
   * if not exist
   */
  private boolean validateCandidateKeyColumn(NodeList tablesNodes, NodeList nodes) {
    boolean hasErrors = false;
    Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();
    for (int i = 0; i < tablesNodes.getLength(); i++) {
      Element tableElement = (Element) tablesNodes.item(i);
      String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
      String schema = XMLUtils.getParentNameByTagName(tableElement, Constants.SCHEMA);

      Element tableColumnsElement = XMLUtils.getChild(tableElement, Constants.COLUMNS);
      if (tableColumnsElement == null) {
        return false;
      }
      NodeList tableColumns = tableColumnsElement.getElementsByTagName(Constants.COLUMN);
      LinkedList<String> tableColumnName = new LinkedList<>();

      for (int j = 0; j < tableColumns.getLength(); j++) {
        tableColumnName.add(XMLUtils.getChildTextContext((Element) tableColumns.item(j), Constants.NAME));
      }
      tableColumnsList.put(schema + table, tableColumnName);
    }

    for (int i = 0; i < nodes.getLength(); i++) {
      Element candidateKey = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(candidateKey, Constants.SCHEMA);
      String table = XMLUtils.getParentNameByTagName(candidateKey, Constants.TABLE);
      NodeList columns = candidateKey.getElementsByTagName(Constants.COLUMN);

      ArrayList<String> columnList = new ArrayList<>();
      for (int k = 0; k < columns.getLength(); k++) {
        String column = columns.item(k).getTextContent();
        columnList.add(column);
      }

      if (columnList.isEmpty()) {
        setError(M_511_1_2, String.format("Candidate key must have at least one column on %s.%s", schema, table));
        if (!this.skipAdditionalChecks) {
          setError(A_M_511_1_2,
            String.format("Aborted because candidate key column is mandatory on %s.%s", schema, table));
          additionalCheckError = true;
        }
        hasErrors = true;
        continue;
      }

      for (String column : columnList) {
        if (column.isEmpty()) {
          setError(M_511_1_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
          if (!this.skipAdditionalChecks) {
            setError(A_M_511_1_2,
              String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
            additionalCheckError = true;
          }
          hasErrors = true;
        } else if (!tableColumnsList.get(schema + table).contains(column) && !this.skipAdditionalChecks) {
          setError(A_M_511_1_2,
            String.format("Candidate key column reference %s not found on %s.%s", column, schema, table));
          additionalCheckError = true;
        }
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.11-1-3 The Candidate key description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   */
  private void validateCandidateKeyDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element candidateKey = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(candidateKey, Constants.SCHEMA),
        Constants.TABLE, XMLUtils.getParentNameByTagName(candidateKey, Constants.TABLE), Constants.CANDIDATE_KEY,
        XMLUtils.getChildTextContext(candidateKey, Constants.NAME));

      String description = XMLUtils.getChildTextContext(candidateKey, Constants.DESCRIPTION);
      validateXMLField(A_M_511_1_3, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
