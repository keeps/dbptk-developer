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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import com.databasepreservation.model.reporters.ValidationReporterStatus;
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

  private List<Element> candidateKeyList = new ArrayList<>();
  private Map<String, LinkedList<String>> tableColumnsList = new HashMap<>();

  public MetadataCandidateKeyValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_511_1, M_511_1_1, A_M_511_1_1, M_511_1_2, A_M_511_1_2, A_M_511_1_3);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_511);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_511, MODULE_NAME);

    validateMandatoryXSDFields(M_511_1, UNIQUE_KEY_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:candidateKeys/ns:candidateKey");

    if (!readXMLMetadataCandidateKeyLevel()) {
      reportValidations(M_511_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (candidateKeyList.isEmpty()) {
      getValidationReporter().skipValidation(M_511_1, "Database has no Candidate keys");
      observer.notifyValidationStep(MODULE_NAME, M_511_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataCandidateKeyLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element tableElement = (Element) nodes.item(i);
        String table = XMLUtils.getChildTextContext(tableElement, Constants.NAME);
        String schema = XMLUtils.getChildTextContext((Element) tableElement.getParentNode().getParentNode(),
          Constants.NAME);

        Element tableColumnsElement = XMLUtils.getChild(tableElement, Constants.COLUMNS);
        if (tableColumnsElement == null) {
          return false;
        }
        NodeList tableColumns = tableColumnsElement.getElementsByTagName(Constants.COLUMN);
        LinkedList<String> tableColumnName = new LinkedList<>();

        for (int j = 0; j < tableColumns.getLength(); j++) {
          tableColumnName.add(XMLUtils.getChildTextContext((Element) tableColumns.item(j), Constants.NAME));
        }
        tableColumnsList.put(table, tableColumnName);

        NodeList candidateKeyNodes = tableElement.getElementsByTagName(Constants.CANDIDATE_KEY);
        if (candidateKeyNodes == null) {
          // next table
          continue;
        }

        for (int j = 0; j < candidateKeyNodes.getLength(); j++) {
          Element candidateKey = (Element) candidateKeyNodes.item(j);
          String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.CANDIDATE_KEY, Integer.toString(j));
          candidateKeyList.add(candidateKey);

          String name = XMLUtils.getChildTextContext(candidateKey, Constants.NAME);
          validateCandidateKeyName(name, path);

          NodeList columns = candidateKey.getElementsByTagName(Constants.COLUMN);

          ArrayList<String> columnList = new ArrayList<>();
          for (int k = 0; k < columns.getLength(); k++) {
            String column = columns.item(k).getTextContent();
            columnList.add(column);
          }

          path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.CANDIDATE_KEY, name);
          validateCandidateKeyColumn(schema, table, columnList);

          String description = XMLUtils.getChildTextContext(candidateKey, Constants.DESCRIPTION);
          validateCandidateKeyDescription(description, path);

        }
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read primary key from SIARD file";
      setError(M_511_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.11-1-1 The Candidate Key name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.11-1-1 validation that candidate key name not contain any blanks
   */
  private boolean validateCandidateKeyName(String name, String path) {
    if(validateXMLField(M_511_1_1, name, Constants.CANDIDATE_KEY, true, false, path)){
      if (name.contains(BLANK)) {
        addWarning(A_M_511_1_1, "Candidate key " + name + " contain blanks in name", path);
      }
      return true;
    }
    setError(A_M_511_1_1, String.format("Aborted because candidate key name is mandatory (%s)", path));
    return false;
  }

  /**
   * M_5.11-1-2 The Candidate Key column is mandatory in SIARD 2.1 specification
   *
   * M_5.11-1-2 The Candidate Key column in SIARD file must exist on table. ERROR
   * if not exist
   */
  private void validateCandidateKeyColumn(String schema, String table, ArrayList<String> columnList) {
    if(columnList.isEmpty()){
      setError(M_511_1_2, String.format("Candidate key must have at least one column on %s.%s",schema, table));
      setError(A_M_511_1_2, String.format("Aborted because candidate key column is mandatory on %s.%s", schema, table));
      return;
    }

    for (String column : columnList) {
      if (column.isEmpty()) {
        setError(M_511_1_2, String.format("Primary key must have at least one column on %s.%s", schema, table));
        setError(A_M_511_1_2, String.format("Aborted because primary key column is mandatory on %s.%s", schema, table));
      }else if (!tableColumnsList.get(table).contains(column)) {
        setError(A_M_511_1_2,
          String.format("Candidate key column reference %s not found on %s.%s", column, schema, table));
      }
    }
  }

  /**
   * A_M_5.11-1-3 The Candidate key description in SIARD file must not be less than
   * 3 characters. WARNING if it is less than 3 characters
   */
  private void validateCandidateKeyDescription(String description, String path) {
    validateXMLField(A_M_511_1_3, description, Constants.DESCRIPTION, false, true, path);
  }
}
