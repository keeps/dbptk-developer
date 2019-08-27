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
public class MetadataRoutineValidator extends MetadataValidator {
  private final String MODULE_NAME;
  private static final String M_515 = "5.15";
  private static final String M_515_1 = "M_5.15-1";
  private static final String M_515_1_1 = "M_5.15-1-1";
  private static final String M_515_1_2 = "M_5.15-1-2";

  private static final String SPECIFIC_NAME = "specificName";

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataRoutineValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_515, MODULE_NAME);

    if (!readXMLMetadataRoutineLevel()) {
      reportValidations(M_515_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_515_1, MODULE_NAME) && reportValidations(M_515_1_1, MODULE_NAME)
      && reportValidations(M_515_1_2, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return false;
    }
    return true;
  }

  private boolean readXMLMetadataRoutineLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element routine = (Element) nodes.item(i);
        String schema = XMLUtils.getParentNameByTagName(routine, Constants.ROUTINE);

        String name = XMLUtils.getChildTextContext(routine, SPECIFIC_NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.ROUTINE, name);
        if (!validateRoutineName(name, path))
          break;

        String description = XMLUtils.getChildTextContext(routine, Constants.DESCRIPTION);
        if (!validateRoutineDescription(description, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
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
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoutineDescription(String description, String path) {
    return validateXMLField(M_515_1_2, description, Constants.DESCRIPTION, false, true, path);
  }
}
