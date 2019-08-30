package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
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
  private static final String M_515_1_2 = "M_5.15-1-2";

  private static final String SPECIFIC_NAME = "specificName";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataRoutineValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_515_1, M_515_1_1, M_515_1_2);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_515);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_515, MODULE_NAME);

    if (!validateMandatoryXSDFields(M_515_1, ROUTINE_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine")) {
      reportValidations(M_515_1, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (!readXMLMetadataRoutineLevel()) {
      reportValidations(M_515_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(MODULE_NAME)) {
      metadataValidationPassed(MODULE_NAME);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataRoutineLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element routine = (Element) nodes.item(i);
        String schema = XMLUtils.getParentNameByTagName(routine, Constants.ROUTINE);
        String path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(routine, SPECIFIC_NAME);
        if (!validateRoutineName(name, path))
          continue; //next view

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
   * M_5.15-1-1 The routine name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoutineName(String name, String path) {
    // M_515_1
    if (!validateXMLField(M_515_1, name, Constants.ROUTINE, true, false, path)) {
      return false;
    }
    // M_5.15-1-1
    if (!checkDuplicates.add(name)) {
      setError(M_515_1_1, String.format("Routine specificName %s must be unique (%s)", name, path));
      return false;
    }

    return true;
  }

  /**
   * M_5.15-1-2 The routine description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateRoutineDescription(String description, String path) {
    validateXMLField(M_515_1_2, description, Constants.DESCRIPTION, false, true, path);
  }
}
