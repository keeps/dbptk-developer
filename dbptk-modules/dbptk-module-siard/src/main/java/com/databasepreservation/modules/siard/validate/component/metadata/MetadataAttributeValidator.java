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
public class MetadataAttributeValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataAttributeValidator.class);
  private final String MODULE_NAME;
  private static final String M_54 = "5.4";
  private static final String M_541 = "M_5.4-1";
  private static final String M_541_1 = "M_5.4-1-1";
  private static final String A_M_541_8 = "A_M_5.4-1-8";

  public MetadataAttributeValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_54);

    getValidationReporter().moduleValidatorHeader(M_54, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,"/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type/ns:attributes/ns:attribute", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read attributes from SIARD file";
      setError(M_541, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_541, "Database has no attributes");
      observer.notifyValidationStep(MODULE_NAME, M_541, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_541, ATTRIBUTE_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:types/ns:type/ns:attributes/ns:attribute");

    if (validateAttributeName(nodes)) {
      validationOk(MODULE_NAME, M_541_1);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_541_1, ValidationReporterStatus.ERROR);
    }

    validateAttributeDescription(nodes);
    validationOk(MODULE_NAME, A_M_541_8);

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.4-1-1 The attribute name is mandatory in SIARD 2.1 specification
   */
  private boolean validateAttributeName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element attribute = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(attribute, Constants.SCHEMA),
        Constants.TYPE, XMLUtils.getParentNameByTagName(attribute, Constants.TYPE), Constants.ATTRIBUTE,
        Integer.toString(i));
      String name = XMLUtils.getChildTextContext(attribute, Constants.NAME);

      if (!validateXMLField(M_541_1, name, Constants.NAME, true, false, path)) {
        hasErrors = true;
      }
    }

    return !hasErrors;
  }

  /**
   * A_M_5.4-1-8 The attribute description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateAttributeDescription(NodeList nodes) {

    for (int i = 0; i < nodes.getLength(); i++) {
      Element attribute = (Element) nodes.item(i);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(attribute, Constants.SCHEMA),
        Constants.TYPE, XMLUtils.getParentNameByTagName(attribute, Constants.TYPE), Constants.ATTRIBUTE,
        XMLUtils.getChildTextContext(attribute, Constants.NAME));
      String description = XMLUtils.getChildTextContext(attribute, Constants.DESCRIPTION);

      validateXMLField(A_M_541_8, description, Constants.DESCRIPTION, false, true, path);
    }
  }
}
