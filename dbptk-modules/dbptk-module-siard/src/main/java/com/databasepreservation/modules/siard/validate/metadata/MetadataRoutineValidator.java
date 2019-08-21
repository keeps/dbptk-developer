package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataRoutineValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Routine level metadata";
  private static final String M_515 = "5.15";
  private static final String M_515_1 = "M_5.15-1";
  private static final String M_515_1_1 = "M_5.15-1-1";
  private static final String M_515_1_2 = "M_5.15-1-2";

  private static final String SPECIFIC_NAME = "specificName";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataRoutineValidator newInstance() {
    return new MetadataRoutineValidator();
  }

  private MetadataRoutineValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_515, MODULE_NAME);
    readXMLMetadataRoutineLevel();
    return reportValidations(M_515_1) && reportValidations(M_515_1_1) && reportValidations(M_515_1_2);
  }

  private boolean readXMLMetadataRoutineLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:routines/ns:routine";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element view = (Element) nodes.item(i);
        String schema = MetadataXMLUtils.getChildTextContext((Element) view.getParentNode().getParentNode(), "name");

        String name = MetadataXMLUtils.getChildTextContext(view, SPECIFIC_NAME);
        if (!validateRoutineName(name, schema))
          break;

        String description = MetadataXMLUtils.getChildTextContext(view, Constants.DESCRIPTION);
        if (!validateRoutineDescription(description, schema, name))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.15-1-1 The routine name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoutineName(String name, String schema) {
    // M_515_1
    if (!validateXMLField(M_515_1, name, Constants.ROUTINE, true, false, Constants.SCHEMA, schema)) {
      return false;
    }
    // M_5.15-1-1
    if (!checkDuplicates.add(name)) {
      setError(M_515_1_1, String.format("Routine specificName %s inside schema %s must be unique", name, schema));
      return false;
    }

    return true;
  }

  /**
   * M_5.15-1-2 The routine description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateRoutineDescription(String description, String schema, String name) {
    return validateXMLField(M_515_1_2, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      SPECIFIC_NAME, name);
  }
}
