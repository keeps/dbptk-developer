package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import com.databasepreservation.Constants;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataParameterValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Parameter level metadata";
  private static final String M_516 = "5.16";
  private static final String M_516_1 = "M_5.16-1";
  private static final String M_516_1_1 = "M_5.16-1-1";
  private static final String M_516_1_2 = "M_5.16-1-2";
  private static final String M_516_1_8 = "M_5.16-1-8";

  private static final String PARAMETER_MODE = "mode";
  private static final String IN = "IN";
  private static final String OUT = "OUT";
  private static final String INOUT = "INOUT";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataParameterValidator newInstance() {
    return new MetadataParameterValidator();
  }

  private MetadataParameterValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_516, MODULE_NAME);
    readXMLMetadataParameterLevel();

    return reportValidations(M_516_1) && reportValidations(M_516_1_1) && reportValidations(M_516_1_2)
      && reportValidations(M_516_1_8);
  }

  private boolean readXMLMetadataParameterLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine/ns:parameters/ns:parameter";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        String schema = MetadataXMLUtils.getParentNameByTagName(view, Constants.SCHEMA);

        String name = MetadataXMLUtils.getChildTextContext(view, Constants.NAME);
        if (!validateParameterName(name, schema))
          break;

        String mode = MetadataXMLUtils.getChildTextContext(view, PARAMETER_MODE);
        if (!validateParameterMode(mode, schema, name))
          break;

        String description = MetadataXMLUtils.getChildTextContext(view, Constants.DESCRIPTION);
        if (!validateParameterDescription(description, schema, name))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.16-1-1 The parameter name in SIARD file should be unique. WARNING when it
   * is empty or not unique
   *
   */
  private boolean validateParameterName(String name, String schema) {
    // M_516_1
    if (name == null || name.isEmpty()) {
      addWarning(M_516_1, String.format("Parameter name should exist inside schema %s", schema));
    }
    // M_5.16-1-1
    if (!checkDuplicates.add(name)) {
      addWarning(M_516_1_1, String.format("Parameter name %s inside schema %s should be unique", name, schema));
    }

    return true;
  }

  /**
   * M_5.16-1-2 The parameter mode in SIARD file should be IN, OUT or INOUT and
   * mandatory. WARNING when it is empty
   *
   */
  private boolean validateParameterMode(String mode, String schema, String name) {
    switch (mode) {
      case IN:
      case OUT:
      case INOUT:
        break;
      default:
        addWarning(M_516_1_2,
          String.format("Parameter '%s' mode '%s' inside schema '%s' is not allowed", name, mode, schema));
        return false;
    }
    return true;
  }

  /**
   * M_5.16-1-8 The parameter description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private boolean validateParameterDescription(String description, String schema, String name) {
    return validateXMLField(M_516_1_8, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.NAME, name);
  }
}
