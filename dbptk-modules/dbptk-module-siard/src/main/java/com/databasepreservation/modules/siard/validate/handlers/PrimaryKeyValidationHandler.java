package com.databasepreservation.modules.siard.validate.handlers;

import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.model.reporters.ValidationReporterStatus;

public class PrimaryKeyValidationHandler extends DefaultHandler {
  private static final String LINE_CONSTANT = "line ";
  private static final String COLUMN_CONSTANT = ", column ";
  private final String columnIndex;
  private final String reqNumber;
  private final String path;
  private ValidationReporter reporter;
  private Locator locator;
  private Map<String, String> primaryKeyData;
  private boolean valid;
  private boolean found;
  private StringBuilder tmp = new StringBuilder();

  public PrimaryKeyValidationHandler(final String path, final String columnIndex, final String requirementNumber,
    final Map<String, String> map, final ValidationReporter reporter) {
    this.columnIndex = columnIndex;
    this.reporter = reporter;
    this.primaryKeyData = map;
    this.reqNumber = requirementNumber;
    this.path = path;
    this.valid = true;
  }

  public boolean getValidationResult() {
    return this.valid;
  }

  @Override
  public void setDocumentLocator(Locator locator) {
    this.locator = locator;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    if (qName.equals(columnIndex)) {
      found = true;
      tmp.setLength(0);
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (qName.equals(columnIndex)) {
      if (primaryKeyData.containsKey(tmp.toString())) {
        reporter.validationStatus(reqNumber, ValidationReporterStatus.ERROR,
          "All the table data (primary data) must meet the consistency requirements of SQL:2008.",
          "Primary key constraint not met, found a duplicate key at line " + locator.getLineNumber() + ", column "
            + locator.getColumnNumber() + " which was also found at " + primaryKeyData.get(tmp.toString())
              + " primary key: '" + tmp.toString() + "'"
              + " in "
            + path);
        valid = false;
      } else {
        primaryKeyData.put(tmp.toString(),
          LINE_CONSTANT + locator.getLineNumber() + COLUMN_CONSTANT + locator.getColumnNumber());
      }
      found = false;
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    if (found) {
      tmp.append(ch, start, length);
    }
  }
}
