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
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataViewValidator extends MetadataValidator {
  private static final String MODULE_NAME = "View level metadata";
  private static final String M_514 = "5.14";
  private static final String M_514_1 = "M_5.14-1";
  private static final String M_514_1_1 = "M_5.14-1-1";
  private static final String M_514_1_2 = "M_5.14-1-2";
  private static final String M_514_1_5 = "M_5.14-1-5";

  private static final int MIN_COLUMN_COUNT = 1;

  private Set<String> checkDuplicates = new HashSet<>();

  public static ValidatorComponentImpl newInstance() {
    return new MetadataViewValidator();
  }

  private MetadataViewValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_514, MODULE_NAME);

    if (!readXMLMetadataViewLevel()) {
      reportValidations(M_514_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_514_1, MODULE_NAME) && reportValidations(M_514_1_1, MODULE_NAME)
      && reportValidations(M_514_1_2, MODULE_NAME) && reportValidations(M_514_1_5, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataViewLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        String schema = MetadataXMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(),
          Constants.NAME);

        String name = MetadataXMLUtils.getChildTextContext(view, Constants.NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW, name);
        if (!validateViewName(name, path))
          break;

        NodeList columnsList = view.getElementsByTagName(Constants.COLUMN);
        if (!validateViewColumn(columnsList, path))
          break;

        String description = MetadataXMLUtils.getChildTextContext(view, Constants.DESCRIPTION);
        if (!validateViewDescription(description, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.14-1-1 The view name in SIARD file must be unique. ERROR when it is empty
   * or not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateViewName(String name, String path) {
    // M_514_1
    if (!validateXMLField(M_514_1, name, Constants.NAME, true, false, path)) {
      return false;
    }
    // M_5.14-1-1
    if (!checkDuplicates.add(name)) {
      setError(M_514_1_1, String.format("View name %s must be unique (%s)", name, path));
      return false;
    }

    return true;
  }

  /**
   * M_5.14-1-2 The view list of columns in SIARD file must have at least one
   * column.
   *
   * @return true if valid otherwise false
   */
  private boolean validateViewColumn(NodeList columns, String path) {
    if (columns.getLength() < MIN_COLUMN_COUNT) {
      setError(M_514_1_2, String.format("View must have at least '%d' column (%s)", MIN_COLUMN_COUNT, path));
    }
    return true;
  }

  /**
   * M_5.14-1-5 The view description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   * 
   * @return true if valid otherwise false
   */
  private boolean validateViewDescription(String description, String path) {
    return validateXMLField(M_514_1_5, description, Constants.DESCRIPTION, false, true, path);
  }

}
