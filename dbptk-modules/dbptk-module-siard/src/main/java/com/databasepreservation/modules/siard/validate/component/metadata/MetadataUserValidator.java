package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

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
public class MetadataUserValidator extends MetadataValidator {
  private static final String MODULE_NAME = "User level metadata";
  private static final String M_518 = "5.18";
  private static final String M_518_1 = "M_5.18-1";
  private static final String M_518_1_1 = "M_5.18-1-1";
  private static final String M_518_1_2 = "M_5.18-1-2";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataUserValidator newInstance() {
    return new MetadataUserValidator();
  }

  private MetadataUserValidator() {
    warnings.clear();
    error.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_518, MODULE_NAME);

    if (!readXMLMetadataUserLevel()) {
      reportValidations(M_518_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_518_1, MODULE_NAME) && reportValidations(M_518_1_1, MODULE_NAME)
      && reportValidations(M_518_1_2, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
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

        String name = MetadataXMLUtils.getChildTextContext(user, Constants.NAME);
        String path = buildPath(Constants.USER, name);
        if (!validateUserName(name, path))
          break;

        String description = MetadataXMLUtils.getChildTextContext(user, Constants.DESCRIPTION);
        if (!validateUserDescription(description, path))
          break;
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.18-1-1 The user name in SIARD file should be unique. ERROR when it is
   * empty, WARNING when it is not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserName(String name, String path) {
    if (!validateXMLField(M_518_1, name, Constants.NAME, true, false, path)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      addWarning(M_518_1_1, String.format("User name %s should be unique", name), path);
    }
    return true;
  }

  /**
   * M_5.18-1-2 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserDescription(String description, String path) {
    return validateXMLField(M_518_1_2, description, Constants.DESCRIPTION, false, true, path);
  }
}
