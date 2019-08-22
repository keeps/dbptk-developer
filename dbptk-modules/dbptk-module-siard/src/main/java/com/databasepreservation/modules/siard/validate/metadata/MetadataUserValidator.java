package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.Constants;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataUserValidator extends MetadataValidator {
  private static final String MODULE_NAME = "User level metadata";
  private static final String M_518 = "5.18";
  private static final String M_518_1 = "M_5.18-1";
  private static final String M_518_1_1 = "M_5.18-1-1";
  private static final String M_518_1_2 = "M_5.18-1-2";

  private Set<String> checkDuplicates = new HashSet<>();

  public static MetadataUserValidator newInstance() {
    return new MetadataUserValidator();
  }

  private MetadataUserValidator() {
    warnings.clear();
    error.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_518, MODULE_NAME);
    if (!readXMLMetadataUserLevel()) {
      setError(M_518_1, "Cannot read users");
    }

    return reportValidations(M_518_1) && reportValidations(M_518_1_1) && reportValidations(M_518_1_2);
  }

  private boolean readXMLMetadataUserLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = Constants.METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:users/ns:user";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element user = (Element) nodes.item(i);

        String name = MetadataXMLUtils.getChildTextContext(user, Constants.NAME);
        if (!validateUserName(name))
          break;

        String description = MetadataXMLUtils.getChildTextContext(user, Constants.DESCRIPTION);
        if (!validateUserDescription(description, name))
          break;
      }
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.18-1-1 The user name in SIARD file should be unique. ERROR when it is
   * empty, WARNING when it is not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserName(String name) {
    if (!validateXMLField(M_518_1, name, Constants.NAME, true, false)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      addWarning(M_518_1_1, String.format("User name %s should be unique", name));
    }
    return true;
  }

  /**
   * M_5.18-1-2 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateUserDescription(String description, String name) {
    return validateXMLField(M_518_1_2, description, Constants.DESCRIPTION, false, true, Constants.USER, name);
  }
}
