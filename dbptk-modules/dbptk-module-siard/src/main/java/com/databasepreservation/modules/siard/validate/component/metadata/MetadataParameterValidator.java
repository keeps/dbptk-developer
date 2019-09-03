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
public class MetadataParameterValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataParameterValidator.class);
  private final String MODULE_NAME;
  private static final String M_516 = "5.16";
  private static final String M_516_1 = "M_5.16-1";
  private static final String M_516_1_1 = "M_5.16-1-1";
  private static final String M_516_1_2 = "M_5.16-1-2";
  private static final String M_516_1_8 = "M_5.16-1-8";

  private static final String IN = "IN";
  private static final String OUT = "OUT";
  private static final String INOUT = "INOUT";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataParameterValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_516_1, M_516_1_1, M_516_1_2, M_516_1_8);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_516);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_516, MODULE_NAME);

    if (!validateMandatoryXSDFields(M_516_1, PARAMETER_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine/ns:parameters/ns:parameter")) {
      reportValidations(M_516_1, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (!readXMLMetadataParameterLevel()) {
      reportValidations(M_516_1, MODULE_NAME);
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

  private boolean readXMLMetadataParameterLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine/ns:parameters/ns:parameter",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element parameter = (Element) nodes.item(i);
        String schema = XMLUtils.getParentNameByTagName(parameter, Constants.SCHEMA);
        String path = buildPath(Constants.SCHEMA, schema, Constants.PARAMETER, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(parameter, Constants.NAME);
        if (!validateParameterName(name, path))
          continue; // next parameter

        path = buildPath(Constants.SCHEMA, schema, Constants.PARAMETER, name);
        String mode = XMLUtils.getChildTextContext(parameter, Constants.PARAMETER_MODE);
        if (!validateParameterMode(mode, path))
          continue; // next parameter

        String description = XMLUtils.getChildTextContext(parameter, Constants.DESCRIPTION);
        validateParameterDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read parameters from SIARD file";
      setError(M_516_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }

  /**
   * M_5.16-1-1 The parameter name in SIARD file should be unique. WARNING when it
   * is empty or not unique
   *
   */
  private boolean validateParameterName(String name, String path) {
    // M_516_1
    if (name == null || name.isEmpty()) {
      addWarning(M_516_1_1, "Parameter name should exist", path);
    }
    // M_5.16-1-1
    if (!checkDuplicates.add(name)) {
      addWarning(M_516_1_1, String.format("Parameter name %s should be unique", name), path);
    }

    return true;
  }

  /**
   * M_5.16-1-2 The parameter mode in SIARD file should be IN, OUT or INOUT and
   * mandatory. WARNING when it is empty
   *
   */
  private boolean validateParameterMode(String mode, String path) {
    switch (mode) {
      case IN:
      case OUT:
      case INOUT:
        break;
      default:
        addWarning(M_516_1_2, String.format("Mode '%s' is not allowed", mode), path);
        return false;
    }
    return true;
  }

  /**
   * M_5.16-1-8 The parameter description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateParameterDescription(String description, String path) {
    validateXMLField(M_516_1_8, description, Constants.DESCRIPTION, false, true, path);
  }
}