package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataXMLUtils {

  static final String TABLE = "table";
  static final String XML_EXTENSION = ".xml";
  static final String SIARD_CONTENT = "content";

  public static Document getDocument(InputStream inputStream)
    throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(inputStream);
  }

  public static XPath setXPath(XPath xPath, final String type) {
    xPath.setNamespaceContext(new NamespaceContext() {
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
        if ("xs".equals(arg0)) {
          return "http://www.w3.org/2001/XMLSchema";
        }
        if ("ns".equals(arg0)) {
          if (TABLE.equals(type)) {
            return "http://www.bar.admin.ch/xmlns/siard/2/table.xsd";
          }
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    return xPath;
  }

  static String createPath(String... parameters) {
    StringBuilder sb = new StringBuilder();
    for (String parameter : parameters) {
      sb.append(parameter).append("/");
    }
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }

  static Element getChild(Element parent, String name) {
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element && name.equals(child.getNodeName())) {
        return (Element) child;
      }
    }
    return null;
  }

  static String getChildTextContext(Element parent, String name) {
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element && name.equals(child.getNodeName())) {
        return child.getTextContent();
      }
    }
    return null;
  }
}
