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
public class MetadataUserValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataUserValidator.class);
  private final String MODULE_NAME;
  private static final String M_517 = "5.17";
  private static final String M_517_1 = "M_5.17-1";
  private static final String M_517_1_1 = "M_5.17-1-1";
  private static final String M_517_1_2 = "M_5.17-1-2";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataUserValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_517_1, M_517_1_1, M_517_1_2);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_517);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_517, MODULE_NAME);

    if (!validateMandatoryXSDFields(M_517_1, USER_TYPE, "/ns:siardArchive/ns:users/ns:user")) {
      reportValidations(M_517_1, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (!readXMLMetadataUserLevel()) {
      reportValidations(M_517_1, MODULE_NAME);
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

  private boolean readXMLMetadataUserLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:users/ns:user", XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element user = (Element) nodes.item(i);
        String path = buildPath(Constants.USER, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(user, Constants.NAME);
        if (!validateUserName(name, path))
          continue; // next user;

        path = buildPath(Constants.USER, name);
        String description = XMLUtils.getChildTextContext(user, Constants.DESCRIPTION);
        validateUserDescription(description, path);
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read users from SIARD file";
      setError(M_517_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.17-1-1 The user name in SIARD file should be unique. ERROR when it is
   * empty, WARNING when it is not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserName(String name, String path) {
    if (!validateXMLField(M_517_1_1, name, Constants.NAME, true, false, path)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      addWarning(M_517_1_1, String.format("User name %s should be unique", name), path);
    }
    return true;
  }

  /**
   * M_5.17-1-2 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserDescription(String description, String path) {
    return validateXMLField(M_517_1_2, description, Constants.DESCRIPTION, false, true, path);
  }
}
