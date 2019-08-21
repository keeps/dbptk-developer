package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataViewValidator extends MetadataValidator {
  private static final String MODULE_NAME = "View level metadata";
  private static final String M_514 = "5.14";
  private static final String M_514_1 = "M_5.14-1";
  private static final String M_514_1_1 = "M_5.14-1-1";
  private static final String M_514_1_2 = "M_5.14-1-2";
  private static final String M_514_1_5 = "M_5.14-1-5";

  private static final String SCHEMA = "schema";
  private static final String VIEW = "view";
  private static final String VIEW_NAME = "name";
  private static final String VIEW_COLUMNS = "column";
  private static final String VIEW_DESCRIPTION = "description";

  private static final int MIN_COLUMN_COUNT = 1;

  private Set<String> checkDuplicates = new HashSet<>();

  public static ValidatorModule newInstance() {
    return new MetadataViewValidator();
  }

  private MetadataViewValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_514, MODULE_NAME);
    readXMLMetadataViewLevel();

    return reportValidations(M_514_1, VIEW) && reportValidations(M_514_1_1, VIEW_NAME)
      && reportValidations(M_514_1_2, VIEW_COLUMNS) && reportValidations(M_514_1_5, VIEW_DESCRIPTION);
  }

  private boolean readXMLMetadataViewLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:views/ns:view";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        String schema = MetadataXMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), "name");

        String name = MetadataXMLUtils.getChildTextContext(view, VIEW_NAME);
        if (!validateViewName(name, schema))
          break;

        NodeList columnsList = view.getElementsByTagName(VIEW_COLUMNS);
        if (!validateViewColumn(columnsList, schema, name))
          break;

        String description = MetadataXMLUtils.getChildTextContext(view, VIEW_DESCRIPTION);
        if (!validateViewDescription(description, schema, name))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.14-1-1 The view name in SIARD file must be unique. ERROR when it is empty
   * or not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateViewName(String name, String schema) {
    // M_514_1
    if (!validateXMLField(name, VIEW, true, false, SCHEMA, schema)) {
      return false;
    }
    // M_5.14-1-1
    if (!checkDuplicates.add(name)) {
      setError(VIEW_NAME, String.format("View name %s inside schema %s must be unique", name, schema));
      return false;
    }

    return true;
  }

  /**
   * M_5.14-1-1 The view list of columns in SIARD file must have at least one
   * column.
   *
   * @return true if valid otherwise false
   */
  private boolean validateViewColumn(NodeList columns, String schema, String name) {
    if (columns.getLength() < MIN_COLUMN_COUNT) {
      setError(VIEW_COLUMNS,
        String.format("View '%s' must have at least '%d' column inside schema:'%s'", name, MIN_COLUMN_COUNT, schema));
    }
    return true;
  }

  /**
   * M_5.14-1-5 The view description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   * 
   * @return true if valid otherwise false
   */
  private boolean validateViewDescription(String description, String schema, String name) {
    return validateXMLField(description, VIEW_DESCRIPTION, false, true, SCHEMA, schema, VIEW_NAME, name);
  }

}
