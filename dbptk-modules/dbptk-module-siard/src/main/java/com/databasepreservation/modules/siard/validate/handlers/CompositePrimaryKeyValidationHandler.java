package com.databasepreservation.modules.siard.validate.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.utils.ListUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CompositePrimaryKeyValidationHandler extends DefaultHandler {
  private static final String COMPOSITE_KEYS_SEPARATOR = "_";
  private final List<String> indexes;
  private final String requirement;
  private final String path;
  private Locator locator;
  private String rowLocation;
  private List<String> compositeKeysSet = new ArrayList<>();
  private Map<String, String> compositeKeysMap;
  private ValidationReporter reporter;
  private boolean valid = true;
  private boolean found;
  private StringBuilder tmp = new StringBuilder();

  public CompositePrimaryKeyValidationHandler(List<String> indexes, Map<String, String> map,
    ValidationReporter reporter, String requirement, String path) {
    this.indexes = indexes;
    this.compositeKeysMap = map;
    this.reporter = reporter;
    this.requirement = requirement;
    this.path = path;
  }

  public boolean getValidationStatus() {
    return valid;
  }

  @Override
  public void setDocumentLocator(Locator locator) {
    this.locator = locator;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) {
    if (indexes.contains(qName)) {
      found = true;
      tmp.setLength(0);
    }

    if (qName.equals("row")) {
      rowLocation = locator.getLineNumber() + ", column " + locator.getColumnNumber();
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) {
    if (indexes.contains(qName)) {
      compositeKeysSet.add(tmp.toString());
      found = false;
    }

    if (qName.equals("row")) {
      if (compositeKeysMap
        .containsKey(ListUtils.convertListToStringWithSeparator(compositeKeysSet, COMPOSITE_KEYS_SEPARATOR))) {
        reporter
          .validationStatus(requirement, ValidationReporterStatus.ERROR,
            "All the table data (primary data) must meet the consistency requirements of SQL:2008.",
            "Composite Primary key duplicated, found on the row block at line " + rowLocation
              + " which was also found at line "
              + compositeKeysMap
                .get(ListUtils.convertListToStringWithSeparator(compositeKeysSet, COMPOSITE_KEYS_SEPARATOR))
                + " composite primary key: (" + ListUtils.convertListToStringWithSeparator(compositeKeysSet, ",") + ")"
              + " in " + path);
        valid = false;
      } else {
        compositeKeysMap.put(ListUtils.convertListToStringWithSeparator(compositeKeysSet, COMPOSITE_KEYS_SEPARATOR),
          rowLocation);
      }

      compositeKeysSet.clear();
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    if (found) {
      tmp.append(ch, start, length);
    }
  }
}
