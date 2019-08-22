package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.modules.siard.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
abstract class MetadataValidator extends ValidatorModule {
  private static final int MIN_FIELD_LENGTH = 3;
  private static final String SEPARATOR = " ";
  private static final String ENTRY = "metadata.xml";

  Map<String, List<String>> warnings = new HashMap<>();
  Map<String, List<String>> notice = new HashMap<>();
  Map<String, String> error = new HashMap<>();

  boolean reportValidations(String codeID) {
    if (error.get(codeID) != null && !error.get(codeID).isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR, error.get(codeID));
      return false;
    } else if ((warnings.get(codeID) != null) && !warnings.get(codeID).isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.WARNING, ENTRY, warnings.get(codeID));
    } else if(notice.get(codeID) != null){
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK, ENTRY, notice.get(codeID));
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
    return true;
  }

  boolean validateXMLField(String codeId, String value, String field, Boolean mandatory, Boolean checkSize,
    String... path) {
    if (!validateMandatoryXMLField(value) && mandatory) {
      setError(codeId, buildMessage(field, value, true, path));
      return false;
    } else if (!validateXMLFieldSize(value) && checkSize) {
      addWarning(codeId, buildMessage(field, value, false, path));
    }
    return true;
  }

  private boolean validateMandatoryXMLField(String value) {
    return value != null && !value.isEmpty();
  }

  private boolean validateXMLFieldSize(String value) {
    return value != null && value.length() >= MIN_FIELD_LENGTH;
  }

  private String buildMessage(String field, String value, Boolean mandatory, String path) {
    if (!validateMandatoryXMLField(value) && mandatory) {
      return String.format("The %s inside '%s' is mandatory", field, path);
    } else if (!validateMandatoryXMLField(value)) {
      return String.format("The %s inside '%s' is null", field, path);
    } else if (!validateXMLFieldSize(value)) {
      return String.format("The %s '%s' inside '%s' has less than %d characters", field, value, path, MIN_FIELD_LENGTH);
    }
    return null;
  }

  private String buildMessage(String field, String value, Boolean mandatory, String... path) {
    return buildMessage(field, value, mandatory, buildPath(path));
  }

  private String buildPath(String... parameters) {
    if(parameters.length < 1){
      return ENTRY;
    }
    StringBuilder path = new StringBuilder();
    for (int i = 0; i < parameters.length; i++) {
      path.append(parameters[i]);
      if (i % 2 != 0 && i < parameters.length - 1) {
        path.append(SEPARATOR).append("and").append(SEPARATOR);
      } else {
        path.append(":");
      }
    }
    path.deleteCharAt(path.length() - 1);

    return path.toString();
  }

  void addWarning(String codeID, String message) {
    if (warnings.get(codeID) == null) {
      warnings.put(codeID, new ArrayList<String>());
    }
    warnings.get(codeID).add(message);
  }

  void addNotice(String codeID, String message){
    if(notice.get(codeID) == null){
      notice.put(codeID, new ArrayList<String>());
    }
    notice.get(codeID).add(message);
  }

  void setError(String codeID, String error) {
    this.error.put(codeID, error);
  }

  NodeList getXPathResult(ZipFile zipFile, String pathToEntry, String xpathExpression, QName constants,
    final String type) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
    NodeList nodes = null;
    final ZipArchiveEntry entry = zipFile.getEntry(pathToEntry);
    final InputStream inputStream = zipFile.getInputStream(entry);

    Document document = MetadataXMLUtils.getDocument(inputStream);

    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();

    xpath = MetadataXMLUtils.setXPath(xpath, type);
    XPathExpression expression = xpath.compile(xpathExpression);

    nodes = (NodeList) expression.evaluate(document, constants);

    return nodes;
  }
}
