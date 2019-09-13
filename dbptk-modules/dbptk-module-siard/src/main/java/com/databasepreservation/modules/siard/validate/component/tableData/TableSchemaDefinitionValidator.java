/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.model.validator.SIARDContent;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TableSchemaDefinitionValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaDefinitionValidator.class);

  private final String MODULE_NAME;
  private static final String P_61 = "T_6.1";
  private static final String P_611 = "T_6.1-1";
  private static final String P_612 = "T_6.1-2";
  private static final String P_613 = "T_6.1-3";
  private static final String P_614 = "T_6.1-4";

  private List<String> P_611_ERRORS = new ArrayList<>();
  private List<String> P_612_ERRORS = new ArrayList<>();
  private List<String> P_613_ERRORS = new ArrayList<>();
  private List<String> P_614_ERRORS = new ArrayList<>();

  @Override
  public void clean() {
    P_611_ERRORS = null;
    P_612_ERRORS = null;
    P_613_ERRORS = null;
    P_614_ERRORS = null;
  }

  public TableSchemaDefinitionValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_61);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_61, MODULE_NAME);

    if (validateXMLSchemaDefinition()) {
      observer.notifyValidationStep(MODULE_NAME, P_611, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_611, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_611, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_611, ValidationReporterStatus.ERROR,
        "There must be an XML schema definition for each table that indicates the XML storage format of the table data.",
        P_611_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateColumnsTag()) {
      observer.notifyValidationStep(MODULE_NAME, P_612, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_612, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_612, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_612, ValidationReporterStatus.ERROR,
        "The column tags always start with c1 and increase by 1. There must be no gap, because a NULL value is expressed by a missing corresponding column in the XML file.",
        P_612_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateXMLSchemaStandardTypes()) {
      observer.notifyValidationStep(MODULE_NAME, P_613, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_613, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_613, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_613, ValidationReporterStatus.ERROR, "Incompatible XML Schema standard types", P_613_ERRORS,
        MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateAdvancedOrStructuredType()) {
      observer.notifyValidationStep(MODULE_NAME, P_614, ValidationReporterStatus.OK);
      getValidationReporter().validationStatus(P_614, ValidationReporterStatus.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_614, ValidationReporterStatus.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.FAILED);
      validationFailed(P_614, ValidationReporterStatus.ERROR,
        "Multiple cell values of advanced or structured types are to be stored as separate elements inside the cell tags.",
        P_614_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);
    closeZipFile();

    return true;
  }

  /**
   * T_6.1-1
   *
   * There must be an XML schema definition for each table that indicates the XML
   * storage format of the table data.
   *
   * @return true if valid otherwise false
   */
  private boolean validateXMLSchemaDefinition() {
    if (preValidationRequirements())
      return false;

    List<SIARDContent> tableData = new ArrayList<>();

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/(schema[0-9]+)/(table[0-9]+)/table[0-9]+)\\.xml$";

      Pattern pattern = Pattern.compile(regexPattern);
      Matcher matcher = pattern.matcher(zipFileName);

      while (matcher.find()) {
        tableData.add(new SIARDContent(matcher.group(2), matcher.group(3)));
      }
    }

    for (SIARDContent content : tableData) {
      final String XSDPath = validatorPathStrategy.getXSDTablePathFromFolder(content.getSchema(),content.getTable());
      if (!getZipFileNames().contains(XSDPath)) {
        P_611_ERRORS.add(validatorPathStrategy.getXMLTablePathFromFolder(content.getSchema(),content.getTable()));
      }
    }

    return P_611_ERRORS.isEmpty();
  }

  /**
   * T_6.1-2
   *
   * This schema definition reflects the SQL schema metadata of the table and
   * indicates that the table is stored as a sequence of lines containing a
   * sequence of column entries with various XML types. The name of the table tag
   * is table, that of the dataset tag is row, while the column tags are called
   * c1, c2, ....
   *
   * The column tags always start with c1 and increase by 1. There must be no gap,
   * because a NULL value is expressed by a missing corresponding column in the
   * XML file.
   * 
   * @return true if valid otherwise false
   */
  private boolean validateColumnsTag() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          String rowName = (String) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:element[@name='table']/xs:complexType/xs:sequence/xs:element/@name", XPathConstants.STRING,
            Constants.NAMESPACE_FOR_TABLE);
          if (StringUtils.isNotBlank(rowName) && !rowName.equals("row"))
            return false;

          NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element", XPathConstants.NODESET,
            Constants.NAMESPACE_FOR_TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            final String name = resultNodes.item(i).getAttributes().getNamedItem("name").getNodeValue();
            if (!validateSequence(name, "^c[0-9]+$", i + 1)) {
              P_612_ERRORS.add("Column tags invalid in " + zipFileName);
            }
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }
    return P_612_ERRORS.isEmpty();
  }

  /**
   * T_6.1-3
   *
   * The type mapping to be used in table schema definitions is specified in
   * P_4.3-3. Apart from XML Schema standard types the following special type are
   * used:
   *
   * clobType, blobType, dateType, timeType, dateTimeType.
   *
   * @return true if valid otherwise false
   */
  private boolean validateXMLSchemaStandardTypes() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element/@type", XPathConstants.NODESET,
            Constants.NAMESPACE_FOR_TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            String type = resultNodes.item(i).getNodeValue();
            if (!type.startsWith("xs")) {
              if (!(type.equals("clobType") || type.equals("blobType") || type.equals("dateType")
                || type.equals("timeType") || type.equals("dateTimeType"))) {
                P_613_ERRORS.add(type + " at " + zipFileName);
              }
            }
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return P_613_ERRORS.isEmpty();
  }

  /**
   * T_6.1-4
   *
   * Multiple cell values of advanced or structured types are to be stored as
   * separate elements inside the cell tags. The names of the individual elements
   * of an ARRAY are a1, a2, .... The names of the individual elements of a UDT
   * are u1, u2, .... The names always start with a1 or u1, respectively, and
   * increase by 1. There must be no gap, because a NULL value is expressed by a
   * missing corresponding column in the XML file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateAdvancedOrStructuredType() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList resultNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[not(@type)]/@name",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);

          for (int i = 0; i < resultNodes.getLength(); i++) {
            final String nodeValue = resultNodes.item(i).getNodeValue();
            String xpathExpression = "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@name='$1']/xs:complexType/xs:sequence/xs:element";
            xpathExpression = xpathExpression.replace("$1", nodeValue);
            P_614_ERRORS.addAll(checkAdvancedOrStructuredSequence(zipFileName, xpathExpression, null));
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return P_614_ERRORS.isEmpty();
  }

  /*
   * Auxiliary Methods
   */
  private List<String> checkAdvancedOrStructuredSequence(String zipFileName, String xpathExpression, String userDefinedColumnName) throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    List<String> errors = new ArrayList<>();

    if (userDefinedColumnName != null) {
      xpathExpression = xpathExpression.concat("[@name='$1']/xs:complexType/xs:sequence/xs:element");
      xpathExpression = xpathExpression.replace("$1", userDefinedColumnName);
    }

    NodeList result = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName), xpathExpression,
      XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
    for (int i = 0; i < result.getLength(); i++) {
      final Node type = result.item(i).getAttributes().getNamedItem("type");
      final String name = result.item(i).getAttributes().getNamedItem("name").getNodeValue();;
      if (type == null) {
        errors.addAll(checkAdvancedOrStructuredSequence(zipFileName, xpathExpression, name));
      }

      if (!validateSequence(name, "^a[0-9]+$", i+1)) {
        if (!validateSequence(name, "u[0-9]+$", i+1)) {
          errors.add("Invalid cell tag at " + zipFileName);
        }
      }
    }
    return errors;
  }

  private boolean validateSequence(String name, String regex, int sequenceValue) {
    if (!name.matches(regex))
      return false;

    int value = Integer.parseInt(name.substring(1));
    return value == sequenceValue;
  }
}
