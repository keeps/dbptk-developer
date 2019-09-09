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

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class DateAndTimestampDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(DateAndTimestampDataValidator.class);

  private final String MODULE_NAME;
  private static final String P_63 = "T_6.3";
  private static final String P_631 = "T_6.3-1";
  private static final String DATE_TYPE_MIN = "0001-01-01Z";
  private static final String DATE_TYPE_MAX = "10000-01-01Z";
  private static final String DATE_TIME_TYPE_MIN = "0001-01-01T00:00:00.000000000Z";
  private static final String DATE_TIME_TYPE_MAX = "10000-01-01T00:00:00.000000000Z";

  private List<String> P_631_ERRORS = new ArrayList<>();

  public DateAndTimestampDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    P_631_ERRORS = null;
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_63);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_63, MODULE_NAME);

    if (validateDatesAndTimestamps()) {
      observer.notifyValidationStep(MODULE_NAME, P_631, Status.OK);
      getValidationReporter().validationStatus(P_631, Status.OK);
    } else {
      observer.notifyValidationStep(MODULE_NAME, P_631, Status.ERROR);
      observer.notifyFinishValidationModule(MODULE_NAME, Status.FAILED);
      validationFailed(P_631, Status.ERROR,
        "Dates and timestamps must be restricted to the years 0001-9999 according to the SQL:2008 specification.",
        P_631_ERRORS, MODULE_NAME);
      closeZipFile();
      return false;
    }

    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }

  /**
   * T_6.3-1
   *
   * Dates and timestamps must be restricted to the years 0001-9999 according to
   * the SQL:2008 specification. This restriction is enforced in the definitions
   * of dateType and dateTimeType.
   *
   * @return true if valid otherwise false
   */
  private boolean validateDatesAndTimestamps() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {

          NodeList dateTypeNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "//xs:element[@type='dateType']", XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          if (dateTypeNodes.getLength() > 1) {
            final String dateTypeMinXPathExpression = "/xs:schema/xs:simpleType[@name='dateType']/xs:restriction/xs:minInclusive/@value";
            final String dateTypeMaxXPathExpression = "/xs:schema/xs:simpleType[@name='dateType']/xs:restriction/xs:maxExclusive/@value";
            if (!validateDateType(zipFileName, dateTypeMinXPathExpression, dateTypeMaxXPathExpression, DATE_TYPE_MIN,
              DATE_TYPE_MAX)) {
              P_631_ERRORS.add("Error on " + zipFileName + " restriction not enforced");
            }
          }

          NodeList dateTimeTypeNodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "//xs:element[@type='dateTimeType']", XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          if (dateTimeTypeNodes.getLength() > 1) {
            final String dateTimeTypeMinXPathExpression = "/xs:schema/xs:simpleType[@name='dateTimeType']/xs:restriction/xs:minInclusive/@value";
            final String dateTimeTypeMaxXPathExpression = "/xs:schema/xs:simpleType[@name='dateTimeType']/xs:restriction/xs:maxExclusive/@value";
            if (!validateDateType(zipFileName, dateTimeTypeMinXPathExpression, dateTimeTypeMaxXPathExpression,
              DATE_TIME_TYPE_MIN, DATE_TIME_TYPE_MAX)) {
              P_631_ERRORS.add("Error on " + zipFileName + " restriction not enforced");
            }
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return P_631_ERRORS.isEmpty();
  }

  private boolean validateDateType(String zipFileName, String minXPathExpression, String maxXPathExpression,
    String minRegex, String maxRegex)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String min = (String) XMLUtils.getXPathResult(getZipInputStream(zipFileName), minXPathExpression,
      XPathConstants.STRING, Constants.NAMESPACE_FOR_TABLE);
    String max = (String) XMLUtils.getXPathResult(getZipInputStream(zipFileName), maxXPathExpression,
      XPathConstants.STRING, Constants.NAMESPACE_FOR_TABLE);

    if (!min.matches(minRegex))
      return false;
    return max.matches(maxRegex);
  }
}
