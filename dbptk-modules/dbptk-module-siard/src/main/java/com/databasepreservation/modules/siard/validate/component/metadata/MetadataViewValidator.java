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
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataViewValidator extends MetadataValidator {
  private final String MODULE_NAME;
  private static final String M_514 = "5.14";
  private static final String M_514_1 = "M_5.14-1";
  private static final String M_514_1_1 = "M_5.14-1-1";
  private static final String M_514_1_2 = "M_5.14-1-2";
  private static final String M_514_1_5 = "M_5.14-1-5";

  private static final int MIN_COLUMN_COUNT = 1;

  private Set<String> checkDuplicates = new HashSet<>();

  public MetadataViewValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_514_1, M_514_1_1, M_514_1_2, M_514_1_5);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_514);
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_514, MODULE_NAME);

    if (!readXMLMetadataViewLevel()) {
      reportValidations(M_514_1, MODULE_NAME);
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

  private boolean readXMLMetadataViewLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        String schema = XMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), Constants.NAME);

        String name = XMLUtils.getChildTextContext(view, Constants.NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW, name);
        if (!validateViewName(name, path))
          break;

        NodeList columnsList = view.getElementsByTagName(Constants.COLUMN);
        if (!validateViewColumn(columnsList, path))
          break;

        String description = XMLUtils.getChildTextContext(view, Constants.DESCRIPTION);
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
