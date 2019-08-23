package com.databasepreservation.modules.siard.validate.component.tableData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TableDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableDataValidator.class);

  private final String MODULE_NAME;
  private static final String P_64 = "T_6.4";
  private static final String P_641 = "T_6.4-1";
  private static final String P_642 = "T_6.4-2";
  private static final String P_643 = "T_6.4-3";
  private static final String P_644 = "T_6.4-4";
  private static final String P_645 = "T_6.4-5";
  private static final String P_646 = "T_6.4-6";
  private static final String TABLE_REGEX = "^table$";
  private static final String ROW_REGEX = "^row$";
  private static final String COLUMN_REGEX = "^c[0-9]+$";
  private static final String ARRAY_REGEX = "^a[0-9]+$";
  private static final String STRUCTURED_REGEX = "^u[0-9]+$";

  public TableDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(P_64, MODULE_NAME);

    if (validateStoredExtensionFile()) {
      getValidationReporter().validationStatus(P_641, Status.OK);
    } else {
      validationFailed(P_641, MODULE_NAME);
      closeZipFile();
      return false;
    }

    if (validateRowElements()) {
      getValidationReporter().validationStatus(P_642, Status.OK);
    } else {
      validationFailed(P_642, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().validationStatus(P_643, Status.OK);

    if (validateLOBAttributes()) {
      getValidationReporter().validationStatus(P_645, Status.OK);
    } else {
      validationFailed(P_645, MODULE_NAME);
      closeZipFile();
      return false;
    }

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }

  /**
   * T_6.4-1
   *
   * The table data for each table must be stored in an XML file.
   *
   * @return true if valid otherwise false
   */
  private boolean validateStoredExtensionFile() {
    if (preValidationRequirements())
      return false;

    List<String> SIARDXMLPaths = new ArrayList<>();

    try {
      NodeList schemaFolders = (NodeList) XMLUtils.getXPathResult(
        getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:folder/text()", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < schemaFolders.getLength(); i++) {
        String schemaFolderName = schemaFolders.item(i).getNodeValue();
        String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema[ns:folder/text()='$1']/ns:tables/ns:table/ns:folder/text()";
        xpathExpression = xpathExpression.replace("$1", schemaFolderName);
        NodeList tableFolders = (NodeList) XMLUtils.getXPathResult(
          getZipInputStream(validatorPathStrategy.getMetadataXMLPath()), xpathExpression, XPathConstants.NODESET,
          Constants.NAMESPACE_FOR_METADATA);

        for (int j = 0; j < tableFolders.getLength(); j++) {
          String path = "content/" + schemaFolderName + "/" + tableFolders.item(j).getNodeValue() + "/"
            + tableFolders.item(j).getNodeValue() + ".xml";
          SIARDXMLPaths.add(path);
        }
      }
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
      return false;
    }

    for (String path : SIARDXMLPaths) {
      if (!getZipFileNames().contains(path)) {
        return false;
      }
    }

    return true;
  }

  /**
   * T_6.4-2
   *
   * The table file consists of row elements containing the data of a line
   * subdivided into the various columns (c1, c2 ...).
   * 
   * @return true if valid otherwise false
   */
  private boolean validateRowElements() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xml$";
      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList nodeNames = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName), "//*",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);

          for (int i = 0; i < nodeNames.getLength(); i++) {
            Element element = (Element) nodeNames.item(i);
            String tagName = element.getTagName();

            if (!(tagName.matches(TABLE_REGEX) || tagName.matches(ROW_REGEX) || tagName.matches(COLUMN_REGEX)
              || tagName.matches(ARRAY_REGEX) || tagName.matches(STRUCTURED_REGEX))) {
              return false;
            }
          }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * T_6.4-5
   *
   * The decision, when to store large object data in separate files rather than
   * inlining them is at the discretion of the software producing the SIARD
   * archive. To avoid creating empty folders, folders are only created when they
   * are necessary, i.e. contain data. If a large object is stored in a separate
   * file, its cell element must have attributes file, length and digest. Here
   * file is a “file:” URI relative to the lobFolder element of the column or
   * attribute metadata. The length contains the length in bytes (for BLOBs) or
   * characters (for CLOBs or XMLs). The digest records a message digest over the
   * LOB file, making it possible to check integrity of the SIARD archive, even
   * when some LOBs are stored externally.
   * 
   * @return true is valid otherwise false
   */
  private boolean validateLOBAttributes() {
    if (preValidationRequirements())
      return false;

    for (String zipFileName : getZipFileNames()) {
      String regexPattern = "^(content/schema[0-9]+/table[0-9]+/table[0-9]+)\\.xsd$";
      if (zipFileName.matches(regexPattern)) {
        try {
          NodeList nodeNames = (NodeList) XMLUtils.getXPathResult(getZipInputStream(zipFileName),
            "/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element[@type='clobType' or @type='blobType']/@name",
            XPathConstants.NODESET, Constants.NAMESPACE_FOR_TABLE);
          for (int i = 0; i < nodeNames.getLength(); i++) {
            final String nodeValue = nodeNames.item(i).getNodeValue();
            // TODO - ASK LOGIC
          }

        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException e) {
          return false;
        }
      }
    }

    return true;
  }
}
