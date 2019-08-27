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
public class MetadataCheckConstraintValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Check Constraint level metadata";
  private static final String M_512 = "5.12";
  private static final String M_512_1 = "M_5.12-1";
  private static final String M_512_1_1 = "M_5.12-1-1";
  private static final String M_512_1_3 = "M_5.12-1-3";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataCheckConstraintValidator newInstance() {
    return new MetadataCheckConstraintValidator();
  }

  private MetadataCheckConstraintValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(M_512, MODULE_NAME);

    if (!readXMLMetadataCheckConstraint()) {
      reportValidations(M_512_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_512_1, MODULE_NAME) && reportValidations(M_512_1_1, MODULE_NAME)
      && reportValidations(M_512_1_3, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataCheckConstraint() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:checkConstraints/ns:checkConstraint",
        XPathConstants.NODESET, Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element checkConstraint = (Element) nodes.item(i);
        Element tableElement = (Element) checkConstraint.getParentNode().getParentNode();
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();

        String schema = MetadataXMLUtils.getChildTextContext(schemaElement, Constants.NAME);
        String table = MetadataXMLUtils.getChildTextContext(tableElement, Constants.NAME);

        String name = MetadataXMLUtils.getChildTextContext(checkConstraint, Constants.NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, Constants.NAME, name);
        if (!validateCheckConstraintName(name, path))
          break;

        String condition = MetadataXMLUtils.getChildTextContext(checkConstraint, Constants.CONDITIONAL);
        // M_512_1
        if (!validateXMLField(M_512_1, condition, Constants.CHECK_CONSTRAINT, true, false, path)) {
          return false;
        }

        String description = MetadataXMLUtils.getChildTextContext(checkConstraint, Constants.DESCRIPTION);
        if (!validateCheckConstraintDescription(description, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.12-1-1 The Check Constraint name in SIARD file must be unique and exist.
   * ERROR if not unique or is null.
   *
   * @return true if valid otherwise false
   */

  private boolean validateCheckConstraintName(String name, String path) {
    // M_512_1
    if (!validateXMLField(M_512_1, name, Constants.CHECK_CONSTRAINT, true, false, path)) {
      return false;
    }
    // M_5.12-1-1
    if (!checkDuplicates.add(name)) {
      setError(M_512_1_1, String.format("Check Constraint name %s must be unique (%s)", name, path));
      return false;
    }

    return true;
  }

  /**
   * M_5.12-1-3 The Check Constraint description in SIARD file must exist. ERROR
   * if not exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateCheckConstraintDescription(String description, String path) {
    return validateXMLField(M_512_1_3, description, Constants.DESCRIPTION, true, true, path);
  }
}
