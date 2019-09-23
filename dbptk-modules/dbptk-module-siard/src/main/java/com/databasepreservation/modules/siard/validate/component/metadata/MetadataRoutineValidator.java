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
import java.util.HashSet;
import java.util.List;
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
public class MetadataRoutineValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataRoutineValidator.class);
  private final String MODULE_NAME;
  private static final String M_515 = "5.15";
  private static final String M_515_1 = "M_5.15-1";
  private static final String M_515_1_1 = "M_5.15-1-1";
  private static final String A_M_515_1_1 = "A_M_5.15-1-1";
  private static final String A_M_515_1_2 = "M_5.15-1-2";

  private static final String SPECIFIC_NAME = "specificName";

  private Set<String> checkDuplicates = new HashSet<>();
  private List<Element> routineList = new ArrayList<>();

  public MetadataRoutineValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_515_1, M_515_1_1, A_M_515_1_1, A_M_515_1_2);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_515);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_515, MODULE_NAME);

    validateMandatoryXSDFields(M_515_1, ROUTINE_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine");

    if (!readXMLMetadataRoutineLevel()) {
      reportValidations(M_515_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (routineList.isEmpty()) {
      getValidationReporter().skipValidation(M_515_1, "Database has no routine");
      observer.notifyValidationStep(MODULE_NAME, M_515_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataRoutineLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element routine = (Element) nodes.item(i);
        routineList.add(routine);
        String schema = XMLUtils.getParentNameByTagName(routine, Constants.SCHEMA);
        String path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(routine, SPECIFIC_NAME);
        validateRoutineName(name, path);

        path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE, name);
        String description = XMLUtils.getChildTextContext(routine, Constants.DESCRIPTION);
        validateRoutineDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read routines from SIARD file";
      setError(M_515_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }

  /**
   * M_515_1_1 is mandatory in SIARD 2.1 specification
   *
   * A_M_5.15-1-1 The routine name in SIARD file must be unique.
   */
  private void validateRoutineName(String name, String path) {
    if(validateXMLField(M_515_1_1, name, Constants.ROUTINE, true, false, path)){
      if (!checkDuplicates.add(name)) {
        setError(A_M_515_1_1, String.format("Routine specificName %s must be unique (%s)", name, path));
      }
      return;
    }
    setError(A_M_515_1_1, String.format("Aborted because specificName is mandatory (%s)", path));
  }

  /**
   * A_M_5.15-1-2 The routine description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateRoutineDescription(String description, String path) {
    validateXMLField(A_M_515_1_2, description, Constants.DESCRIPTION, false, true, path);
  }
}
