package com.databasepreservation.modules.siard.validate.TableData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class DateAndTimestampDataValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(DateAndTimestampDataValidator.class);

  private static final String MODULE_NAME = " Date and timestamp data cells";
  private static final String P_63 = "T_6.3";
  private static final String P_631 = "T_6.3-1";
  private static final String TABLE = "table";
  private static final String DATE_TYPE_MIN = "0001-01-01Z";
  private static final String DATE_TYPE_MAX = "10000-01-01Z";
  private static final String DATE_TIME_TYPE_MIN = "0001-01-01T00:00:00.000000000Z";
  private static final String DATE_TIME_TYPE_MAX = "10000-01-01T00:00:00.000000000Z";

  private static ZipFile zipFile = null;
  private static List<String> zipFileNames = null;

  public static DateAndTimestampDataValidator newInstance() {
    return new DateAndTimestampDataValidator();
  }

  private DateAndTimestampDataValidator() {
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(P_63, MODULE_NAME);

    if (validateDatesAndTimestamps()) {
      getValidationReporter().validationStatus(P_631, ValidationReporter.Status.OK);
    } else {
      validationFailed(P_631, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.OK);
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

    if (zipFileNames == null) {
      try {
        retrieveFilesInsideZip();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    for (String zipFileName : zipFileNames) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";

      if (zipFileName.matches(regexPattern)) {
        try {

          NodeList dateTypeNodes = (NodeList) getXPathResult(zipFileName, "//xs:element[@type='dateType']", XPathConstants.NODESET, TABLE);
          if (dateTypeNodes.getLength() > 1) {
            final String dateTypeMinXPathExpression = "/xs:schema/xs:simpleType[@name='dateType']/xs:restriction/xs:minInclusive/@value";
            final String dateTypeMaxXPathExpression = "/xs:schema/xs:simpleType[@name='dateType']/xs:restriction/xs:maxExclusive/@value";
            if (!validateDateType(zipFileName, dateTypeMinXPathExpression, dateTypeMaxXPathExpression, DATE_TYPE_MIN, DATE_TYPE_MAX))
              return false;
          }

          NodeList dateTimeTypeNodes = (NodeList) getXPathResult(zipFileName, "//xs:element[@type='dateTimeType']", XPathConstants.NODESET, TABLE);
          if (dateTimeTypeNodes.getLength() > 1) {
            final String dateTimeTypeMinXPathExpression = "/xs:schema/xs:simpleType[@name='dateTimeType']/xs:restriction/xs:minInclusive/@value";
            final String dateTimeTypeMaxXPathExpression = "/xs:schema/xs:simpleType[@name='dateTimeType']/xs:restriction/xs:maxExclusive/@value";
            if (!validateDateType(zipFileName, dateTimeTypeMinXPathExpression, dateTimeTypeMaxXPathExpression, DATE_TIME_TYPE_MIN, DATE_TIME_TYPE_MAX))
              return false;
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
  }

  private boolean validateDateType(String zipFileName, String minXPathExpression, String maxXPathExpression,
    String minRegex, String maxRegex)
    throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
    String min = (String) getXPathResult(zipFileName, minXPathExpression, XPathConstants.STRING, TABLE);
    String max = (String) getXPathResult(zipFileName, maxXPathExpression, XPathConstants.STRING, TABLE);

    if (!min.matches(minRegex))
      return false;
    if (!max.matches(maxRegex))
      return false;

    return true;
  }

  /*
   * Auxiliary Methods
   */
  private boolean preValidationRequirements() {
    if (getSIARDPackagePath() == null) {
      return true;
    }

    if (zipFile == null) {
      try {
        getZipFile();
      } catch (IOException e) {
        return true;
      }
    }

    return false;
  }

  private void getZipFile() throws IOException {
    if (zipFile == null)
      zipFile = new ZipFile(getSIARDPackagePath().toFile());
  }

  private void retrieveFilesInsideZip() throws IOException {
    zipFileNames = new ArrayList<>();
    if (zipFile == null) {
      getZipFile();
    }
    final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
    while (entries.hasMoreElements()) {
      zipFileNames.add(entries.nextElement().getName());
    }
  }

  private void closeZipFile() throws ModuleException {
    if (zipFile != null) {
      try {
        zipFile.close();
      } catch (IOException e) {
        throw new ModuleException().withCause(e.getCause()).withMessage("Error trying to close the SIARD file");
      }
      zipFile = null;
    }
  }

  private Object getXPathResult(final String pathToEntry, final String xpathExpression, QName constants,
    final String type) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
    final ZipArchiveEntry entry = zipFile.getEntry(pathToEntry);
    final InputStream inputStream = zipFile.getInputStream(entry);
    Document document = getDocument(inputStream);

    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = setXPath(xpath, type);

    XPathExpression expression = xpath.compile(xpathExpression);

    return expression.evaluate(document, constants);
  }

  private static Document getDocument(InputStream inputStream)
    throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(inputStream);
  }

  private static XPath setXPath(XPath xPath, final String type) {
    xPath.setNamespaceContext(new NamespaceContext() {
      @Override
      public Iterator getPrefixes(String arg0) {
        return null;
      }

      @Override
      public String getPrefix(String arg0) {
        return null;
      }

      @Override
      public String getNamespaceURI(String arg0) {
        if ("xs".equals(arg0)) {
          return "http://www.w3.org/2001/XMLSchema";
        }
        if ("ns".equals(arg0)) {
          if (TABLE.equals(type)) {
            return "http://www.bar.admin.ch/xmlns/siard/2/table.xsd";
          }
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    return xPath;
  }
}
