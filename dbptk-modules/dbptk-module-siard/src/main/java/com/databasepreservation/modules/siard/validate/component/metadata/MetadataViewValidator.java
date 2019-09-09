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
public class MetadataViewValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataViewValidator.class);
  private final String MODULE_NAME;
  private static final String M_514 = "5.14";
  private static final String M_514_1 = "M_5.14-1";
  private static final String M_514_1_1 = "M_5.14-1-1";
  private static final String M_514_1_2 = "M_5.14-1-2";
  private static final String A_M_514_1_1 = "A_M_5.14-1-1";
  private static final String A_M_514_1_2 = "A_M_5.14-1-1";
  private static final String A_M_514_1_5 = "A_M_5.14-1-5";

  private static final int MIN_COLUMN_COUNT = 1;

  private Set<String> checkDuplicates = new HashSet<>();
  private List<Element> viewList = new ArrayList<>();

  public MetadataViewValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_514_1, M_514_1_1, A_M_514_1_1, M_514_1_2, A_M_514_1_2, A_M_514_1_5);
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_514);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_514, MODULE_NAME);

    validateMandatoryXSDFields(M_514_1, VIEW_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view");

    if (!readXMLMetadataViewLevel()) {
      reportValidations(M_514_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (viewList.isEmpty()) {
      getValidationReporter().skipValidation(M_514_1, "Database has no view");
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataViewLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        viewList.add(view);
        String schema = XMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), Constants.NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.VIEW, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(view, Constants.NAME);
        validateViewName(name, path);
        path = buildPath(Constants.SCHEMA, schema, Constants.VIEW, name);

        NodeList columnsList = view.getElementsByTagName(Constants.COLUMN);
        validateViewColumn(columnsList, path);

        String description = XMLUtils.getChildTextContext(view, Constants.DESCRIPTION);
        validateViewDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read views from SIARD file";
      setError(M_514_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }
    return true;
  }

  /**
   * M_5.14-1-1 The view name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.14-1-1 The view name in SIARD file must be unique. ERROR when it is empty
   * or not unique
   */
  private void validateViewName(String name, String path) {
    if(validateXMLField(M_514_1_1, name, Constants.NAME, true, false, path)){
      if (!checkDuplicates.add(name)) {
        setError(A_M_514_1_1, String.format("View name %s must be unique (%s)", name, path));
      }
      return;
    }
    setError(A_M_514_1_1, String.format("Aborted because view name is mandatory (%s)", path));
  }

  /**
   * M_5.14-1-2 The view list of columns is mandatory in siard, and must exist on table column.
   *
   * A_M_5.14-1-2 should exist at least one column.
   */
  private void validateViewColumn(NodeList columns, String path) {
    //TODO check if column exist M_5.14-1-2
    if (columns.getLength() < MIN_COLUMN_COUNT) {
      setError(A_M_514_1_2, String.format("View must have at least '%d' column (%s)", MIN_COLUMN_COUNT, path));
    }
  }

  /**
   * A_M_5.14-1-5 The view description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateViewDescription(String description, String path) {
    validateXMLField(A_M_514_1_5, description, Constants.DESCRIPTION, false, true, path);
  }

}
