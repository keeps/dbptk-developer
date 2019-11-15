/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.handlers;

import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.common.ValidationObserver;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.model.reporters.ValidationReporterStatus;

public class PrimaryKeyValidationHandler extends DefaultHandler {
  private static final String SIARD_ROW = "row";
  private static final int SPARSE_PROGRESS_MINIMUM_TIME = 3000;
  private static final int SPARSE_PROGRESS_MINIMUM_ROWS = 1000;
  private static final String LINE_CONSTANT = "line ";
  private static final String COLUMN_CONSTANT = ", column ";
  private final String columnIndex;
  private final String reqNumber;
  private final String path;
  private final ValidationReporter reporter;
  private final ValidationObserver observer;
  private Locator locator;
  private Map<String, String> primaryKeyData;
  private boolean valid;
  private boolean found;
  private StringBuilder tmp = new StringBuilder();
  private int rowCount = 0;
  private long lastSparseProgressTimestamp = 0;

  public PrimaryKeyValidationHandler(final String path, final String columnIndex, final String requirementNumber,
    final Map<String, String> map, final ValidationReporter reporter, final ValidationObserver observer) {
    this.columnIndex = columnIndex;
    this.reporter = reporter;
    this.observer = observer;
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

    if (qName.equals(SIARD_ROW)) {
      // notify sparse
      if (rowCount != 0 && rowCount % SPARSE_PROGRESS_MINIMUM_ROWS == 0
        && System.currentTimeMillis() - lastSparseProgressTimestamp > SPARSE_PROGRESS_MINIMUM_TIME) {
        lastSparseProgressTimestamp = System.currentTimeMillis();
        observer.notifyValidationProgressSparse(rowCount);
      }

      rowCount++;
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    if (found) {
      tmp.append(ch, start, length);
    }
  }
}
