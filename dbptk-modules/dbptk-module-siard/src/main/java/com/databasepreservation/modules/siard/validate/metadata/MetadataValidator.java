package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
abstract class MetadataValidator extends ValidatorModule {
  private static final int MIN_FIELD_LENGTH = 3;
  private static final String SEPARATOR = " ";

  List<String> hasWarnings = new ArrayList<>();
  String hasErrors = null;

  boolean reportValidations(boolean result, String codeID, boolean mandatory) {
    if (!result && mandatory) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR, hasErrors);
      return false;
    } else if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
    return true;
  }

  boolean validateMandatoryXMLField(String field, String fieldName, boolean validateSize) {
    if (field == null || field.isEmpty()) {
      hasErrors = fieldName + SEPARATOR + field;
      return false;
    }
    if (validateSize) {
      validateXMLFieldSize(field, fieldName);
    }
    return true;
  }

  void validateXMLFieldSize(String field, String fieldName) {
    if (field == null || field.length() < MIN_FIELD_LENGTH) {
      hasWarnings.add(fieldName + SEPARATOR + field);
    }
  }

  boolean validateMandatoryXMLFieldList(List<String> fieldList, String fieldName, boolean validateSize) {
    hasWarnings.clear();
    for (String field : fieldList) {
      if (!validateMandatoryXMLField(field, fieldName, validateSize)) {
        hasErrors = fieldName + SEPARATOR + field;
        return false;
      }
    }
    return true;
  }

  void validateXMLFieldSizeList(List<String> fieldList, String fieldName) {
    hasWarnings.clear();
    for (String field : fieldList) {
      validateXMLFieldSize(field, fieldName);
    }
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
