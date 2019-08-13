package com.databasepreservation.modules.siard.validate.metadata;

import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.BINARY_LARGE_OBJECT;
import static com.databasepreservation.modules.siard.constants.SIARDDKConstants.CHARACTER_LARGE_OBJECT;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataColumnsValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Column level metadata";
  private static final String M_56 = "5.6";
  private static final String M_561 = "M_5.6-1";
  private static final String M_561_1 = "M_5.6-1-1";
  private static final String M_561_3 = "M_5.6-1-3";
  private static final String BLOB = "BLOB";
  private static final String CLOB = "CLOB";
  private static final String XML = "XML";

  private List<Element> columnsList = new ArrayList<>();
  private List<String> nameList = new ArrayList<>();
  private List<Map<String, String>> lobFolderList = new ArrayList<>();
  private List<String> descriptionList = new ArrayList<>();
  private Set<String> typeOriginalSet = new HashSet<>();

  public static ValidatorModule newInstance() {
    return new MetadataColumnsValidator();
  }

  private MetadataColumnsValidator() {

  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_56, MODULE_NAME);

    if (!reportValidations(readXMLMetadataColumnLevel(), M_561, true)) {
      return false;
    }

    if (!reportValidations(validateColumnName(), M_561_1, true)) {
      return false;
    }

    if (!reportValidations(validateColumnLobFolder(), M_561_3, true)) {
      return false;
    }

    return false;
  }

  private boolean readXMLMetadataColumnLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      final ZipArchiveEntry metadataEntry = zipFile.getEntry("header/metadata.xml");
      final InputStream inputStream = zipFile.getInputStream(metadataEntry);
      Document document = MetadataXMLUtils.getDocument(inputStream);
      String xpathExpressionDatabase = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:columns/ns:column";

      XPathFactory xPathFactory = XPathFactory.newInstance();
      XPath xpath = xPathFactory.newXPath();

      xpath = MetadataXMLUtils.setXPath(xpath);

      try {
        XPathExpression expr = xpath.compile(xpathExpressionDatabase);
        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
          Element column = (Element) nodes.item(i);
          columnsList.add(column);

          Element nameElement = MetadataXMLUtils.getChild(column, "name");
          String name = nameElement != null ? nameElement.getTextContent() : null;
          nameList.add(name);

          Element lobFolderElement = MetadataXMLUtils.getChild(column, "lobFolder");
          String lobFolder = lobFolderElement != null ? lobFolderElement.getTextContent() : null;

          Element typeElement = MetadataXMLUtils.getChild(column, "type");
          String type = typeElement != null ? typeElement.getTextContent() : null;

          Map<String, String> lobFolderMap = new HashMap<>();
          lobFolderMap.put("folder", lobFolder);
          lobFolderMap.put("type", type);
          Element schema = (Element) column.getParentNode().getParentNode().getParentNode();
          lobFolderList.add(lobFolderMap);

          Element typeOriginalElement = MetadataXMLUtils.getChild(column, "typeOriginal");
          String typeOriginal = typeOriginalElement != null ? typeOriginalElement.getTextContent() : null;
          typeOriginalSet.add(typeOriginal);

          Element descriptionElement = MetadataXMLUtils.getChild(column, "description");
          String description = descriptionElement != null ? descriptionElement.getTextContent() : null;
          descriptionList.add(description);

        }
      } catch (XPathExpressionException e) {
        return false;
      }

    } catch (IOException | ParserConfigurationException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.6-1-1 The column name in SIARD file must not be empty.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnName() {
    return validateMandatoryXMLFieldList(nameList, "name", false);
  }

  /**
   * M_5.6-1-3 If the column is large object type (e.g. BLOB, CLOB or XML) which
   * are stored internally in the SIARD archive and the column has data in it then
   * this element should exist.
   *
   * @return true if valid otherwise false
   */
  private boolean validateColumnLobFolder() {
    for (Map<String, String> lobFolder : lobFolderList) {
      String type = lobFolder.get("type");
      String folder = lobFolder.get("folder");

      if (type == null) {
        continue;
      }
      if (type.equals(CHARACTER_LARGE_OBJECT) || type.equals(BINARY_LARGE_OBJECT) || type.equals(BLOB)
        || type.equals(CLOB) || type.equals(XML)) {
        if (folder == null || folder.isEmpty()) {
          hasErrors = "lobFolder must be set for column type " + type;
          return false;
        } else {

        }
      }
    }
    return true;
  }
}
