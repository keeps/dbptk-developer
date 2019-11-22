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
import java.util.HashSet;
import java.util.Set;

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
public class MetadataRoutineValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataRoutineValidator.class);
  private final String MODULE_NAME;
  private static final String M_515 = "5.15";
  private static final String M_515_1 = "M_5.15-1";
  private static final String M_515_1_1 = "M_5.15-1-1";
  private static final String A_M_515_1_1 = "A_M_5.15-1-1";
  private static final String A_M_515_1_2 = "M_5.15-1-2";

  private static final String SPECIFIC_NAME = "specificName";
  private boolean additionalCheckError = false;
  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataRoutineValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {

    observer.notifyStartValidationModule(MODULE_NAME, M_515);
    getValidationReporter().moduleValidatorHeader(M_515, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read routines from SIARD file";
      setError(M_515_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_515_1, "Database has no routine");
      observer.notifyValidationStep(MODULE_NAME, M_515_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_515_1, ROUTINE_TYPE, "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine");

    if (validateRoutineName(nodes)) {
      validationOk(MODULE_NAME, M_515_1_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_515_1_1, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_515_1_1, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_515_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      if (!additionalCheckError) {
        validationOk(MODULE_NAME, A_M_515_1_1);
      } else {
        observer.notifyValidationStep(MODULE_NAME, A_M_515_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_515_1_2, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_515_1_2, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateRoutineDescription(nodes);
      validationOk(MODULE_NAME, A_M_515_1_2);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_515_1_1 is mandatory in SIARD 2.1 specification
   *
   * A_M_5.15-1-1 The routine name in SIARD file must be unique.
   */
  private boolean validateRoutineName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element routine = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(routine, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE, Integer.toString(i));

      String name = XMLUtils.getChildTextContext(routine, SPECIFIC_NAME);
      if (validateXMLField(M_515_1_1, name, Constants.ROUTINE, true, false, path)) {
        if (!checkDuplicates.add(name) && !this.skipAdditionalChecks) {
          setError(A_M_515_1_1, String.format("Routine specificName %s must be unique (%s)", name, path));
          additionalCheckError = true;
          hasErrors = true;
        }
        continue;
      }
      if (!this.skipAdditionalChecks) {
        setError(A_M_515_1_1, String.format("Aborted because specificName is mandatory (%s)", path));
        additionalCheckError = true;
      }
      hasErrors = true;
    }

    return !hasErrors;
  }

  /**
   * A_M_5.15-1-2 The routine description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateRoutineDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element routine = (Element) nodes.item(i);
      String schema = XMLUtils.getParentNameByTagName(routine, Constants.SCHEMA);
      String path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE,
        XMLUtils.getChildTextContext(routine, SPECIFIC_NAME));

      String description = XMLUtils.getChildTextContext(routine, Constants.DESCRIPTION);

      validateXMLField(A_M_515_1_2, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
