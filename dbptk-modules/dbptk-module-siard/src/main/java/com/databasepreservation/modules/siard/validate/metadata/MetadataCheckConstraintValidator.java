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

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataCheckConstraintValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Check Constraint level metadata";
  private static final String M_512 = "5.12";
  private static final String M_512_1 = "M_5.12-1";
  private static final String M_512_1_1 = "M_5.12-1-1";
  private static final String M_512_1_3 = "M_5.12-1-3";

  private static final String SCHEMA = "schema";
  private static final String TABLE = "table";
  private static final String CHECK_CONSTRAINT = "checkConstraint";
  private static final String CHECK_CONSTRAINT_NAME = "name";
  private static final String CHECK_CONSTRAINT_CONDITIONAL = "conditional";
  private static final String CHECK_CONSTRAINT_DESCRIPTION = "description";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataCheckConstraintValidator newInstance() {
    return new MetadataCheckConstraintValidator();
  }

  private MetadataCheckConstraintValidator() {
    error.clear();
    warnings.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_512, MODULE_NAME);
    readXMLMetadataCheckConstraint();

    return reportValidations(M_512_1, CHECK_CONSTRAINT) && reportValidations(M_512_1_1, CHECK_CONSTRAINT_NAME)
      && reportValidations(M_512_1_3, CHECK_CONSTRAINT_DESCRIPTION);
  }

  private boolean readXMLMetadataCheckConstraint() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:checkConstraints/ns:checkConstraint";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element checkConstraint = (Element) nodes.item(i);
        Element tableElement = (Element) checkConstraint.getParentNode().getParentNode();
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();

        String schema = MetadataXMLUtils.getChildTextContext(schemaElement, "name");
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");

        String name = MetadataXMLUtils.getChildTextContext(checkConstraint, CHECK_CONSTRAINT_NAME);
        if (!validateCheckConstraintName(name, schema, table))
          break;

        String condition = MetadataXMLUtils.getChildTextContext(checkConstraint, CHECK_CONSTRAINT_CONDITIONAL);
        // M_512_1
        if (!validateXMLField(condition, CHECK_CONSTRAINT, true, false, SCHEMA, schema, TABLE, table)) {
          return false;
        }

        String description = MetadataXMLUtils.getChildTextContext(checkConstraint, CHECK_CONSTRAINT_DESCRIPTION);
        if (!validateCheckConstraintDescription(description, schema, table, name))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }
    return true;
  }

  /**
   * M_5.12-1-1 The Check Constraint name in SIARD file must be unique and exist.
   * ERROR if not unique or is null.
   *
   * @return true if valid otherwise false
   */

  private boolean validateCheckConstraintName(String name, String schema, String table) {
    // M_512_1
    if (!validateXMLField(name, CHECK_CONSTRAINT, true, false, SCHEMA, schema, TABLE, table)) {
      return false;
    }
    // M_5.12-1-1
    if (!checkDuplicates.add(name)) {
      setError(CHECK_CONSTRAINT_NAME,
        String.format("Check Constraint name %s inside %s.%s must be unique", name, schema, table));
      return false;
    }

    return true;
  }

  /**
   * M_5.12-1-3 The Check Constraint description in SIARD file must exist. ERROR
   * if not exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateCheckConstraintDescription(String description, String schema, String table, String name) {
    return validateXMLField(description, CHECK_CONSTRAINT_DESCRIPTION, true, true, SCHEMA, schema, TABLE, table,
      CHECK_CONSTRAINT_NAME, name);
  }
}
