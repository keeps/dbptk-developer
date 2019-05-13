/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.read.CloseableIterable;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.NamespaceSupport;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDImportEdit implements EditModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private final ContentImportStrategy contentStrategy;
  private final MetadataImportStrategy metadataStrategy;
  private ModuleSettings moduleSettings;

  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDImportEdit.class);

  private static final String METADATA_FILENAME = "metadata";

  public SIARDImportEdit(ContentImportStrategy contentStrategy, SIARDArchiveContainer mainContainer,
    ReadStrategy readStrategy, MetadataImportStrategy metadataStrategy) {
    this.readStrategy = readStrategy;
    this.mainContainer = mainContainer;
    this.contentStrategy = contentStrategy;
    this.metadataStrategy = metadataStrategy;
  }

  @Override
  public DatabaseStructure getMetadata() throws ModuleException {
    moduleSettings = new ModuleSettings();

    LOGGER.info("Importing SIARD version {}", mainContainer.getVersion().getDisplayName());
    DatabaseStructure dbStructure;

    try {
      metadataStrategy.loadMetadata(readStrategy, mainContainer, moduleSettings);

      dbStructure = metadataStrategy.getDatabaseStructure();

    } finally {
      readStrategy.finish(mainContainer);
    }
    return dbStructure;
  }

  public List<String> getXSD() throws ModuleException {
    SIARD2MetadataPathStrategy siard2MetadataPathStrategy = new SIARD2MetadataPathStrategy();
    ZipReadStrategy zipReadStrategy = new ZipReadStrategy();
    zipReadStrategy.setup(mainContainer);

    List<String> validMetadataKeys = new ArrayList<>();

    try (InputStream xmlStream = zipReadStrategy.createInputStream(mainContainer, "header/metadata.xml")) {

      Document doc = getDocument(xmlStream);

      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:name/text()";

      System.out.println(evaluateXPath(doc, xpathExpression));

    } catch (IOException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      e.printStackTrace();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }

    try (InputStream xsdStream = zipReadStrategy.createInputStream(mainContainer,
      siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))) {

      Document doc = getDocument(xsdStream);

      // String xpathExpression =
      // "//element[@name='siardArchive']/complexType/sequence/element/@name";
      // validMetadataKeys = evaluateXPath(doc, xpathExpression);

      NodeList list = doc.getElementsByTagName("xs:element");
      for (int i = 0; i < list.getLength(); i++) {
        Element first = (Element) list.item(i);
        if (first.getAttribute("name") != null) {

          if (first.getParentNode().getParentNode() != null) {
            if (first.getParentNode().getParentNode().getParentNode() != null) {

              Element e = (Element) first.getParentNode().getParentNode().getParentNode();

              if (e.getAttribute("name").equals("siardArchive")) {
                validMetadataKeys.add(first.getAttribute("name"));
              }
            }
          }
        }
      }
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + siard2MetadataPathStrategy.getXsdFilePath(METADATA_FILENAME))
        .withCause(e);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return validMetadataKeys;
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    metadataStrategy.setOnceReporter(reporter);
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }

  private static List<String> evaluateXPath(Document document, String xpathExpression) {
    XPathFactory xPathFactory = XPathFactory.newInstance();

    XPath xpath = xPathFactory.newXPath();

    xpath.setNamespaceContext(new NamespaceContext() {
      @Override
      public Iterator getPrefixes(String arg0) {
        return null;
      }
      @Override
      public String getPrefix(String arg0) {
        return null;
      }
      @Override
      public String getNamespaceURI(String arg0) {
        if ("ns".equals(arg0)) {
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    List<String> values = new ArrayList<>();

    try {
      XPathExpression expr = xpath.compile(xpathExpression);

      NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);

      for (int i = 0; i < nodes.getLength(); i++) {
        values.add(nodes.item(i).getNodeValue());
      }
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }

    return values;
  }

  private static Document getDocument(InputStream inputStream)
    throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    Document doc = builder.parse(inputStream);

    return doc;
  }

  public static void convertInputStreamToFileCommonWay(InputStream is) throws IOException {
    OutputStream outputStream = null;
    try {
      File file = new File("/home/mguimaraes/Desktop/dbptk-metadata.xsd");
      outputStream = new FileOutputStream(file);

      int read = 0;
      byte[] bytes = new byte[1024];
      while ((read = is.read(bytes)) != -1) {
        outputStream.write(bytes, 0, read);
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }
}
