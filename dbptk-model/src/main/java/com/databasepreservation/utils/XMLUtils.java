/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Iterator;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.translate.AggregateTranslator;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.commons.lang3.text.translate.UnicodeEscaper;
import org.apache.commons.lang3.text.translate.UnicodeUnescaper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class XMLUtils {
  /**
   * Translator to escape characters according to the SIARD specification
   */
  private static final CharSequenceTranslator SIARD_ESCAPE = new AggregateTranslator(

    new SIARDUnicodeEscaper(),

    new LookupTranslator(EntityArrays.BASIC_ESCAPE()), new LookupTranslator(EntityArrays.APOS_ESCAPE()));

  /**
   * Translator to convert escaped text in a SIARD file back to unescaped text
   */
  private static final CharSequenceTranslator SIARD_UNESCAPE = new AggregateTranslator(

    new LookupTranslator(EntityArrays.APOS_UNESCAPE()), new LookupTranslator(EntityArrays.BASIC_UNESCAPE()),

    new UnicodeUnescaper());

  /**
   * Encodes a data string as defined by SIARD formats
   *
   * @param text
   *          the text string to be encoded
   * @return the encoded (XML-safe) text
   */
  public static final String encode(String text) {
    // allowed characters by XML spec:
    // #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] |
    // [#x10000-#x10FFFF]
    text = SIARD_ESCAPE.translate(text);

    // all ' ' (space character) were replaced by "\u0020".
    // single spaces must be converted back to a ' ' character.
    // multiple spaces must continue as sequences of "\u0020"
    // text = text.replaceAll("(?<!\\\\u0020)\\\\u0020(?!\\\\u0020)", " ");
    text = spaceEscaper(text);

    return text;
  }

  /**
   * Decodes an encoded string as defined by SIARD formats
   *
   * @param text
   *          the encoded string
   * @return the original string
   */
  public static String decode(String text) {
    if (text == null) {
      return null;
    }

    text = SIARD_UNESCAPE.translate(text);
    return text;
  }

  private static String spaceEscaper(final CharSequence input) {
    if (input == null) {
      return null;
    }
    try {
      final StringWriter out = new StringWriter(input.length() * 2);
      int pos = 0;
      final int len = input.length();
      int spaces = 0;
      while (pos < len) {
        int codePoint = Character.codePointAt(input, pos);
        if (codePoint != 0x20) {
          if (spaces > 0) {
            if (spaces == 1) {
              out.write(' ');
            } else {
              out.write(StringUtils.repeat("\\u0020", spaces));
            }
            spaces = 0;
          }

          final char[] c = Character.toChars(Character.codePointAt(input, pos));
          out.write(c);
          pos += c.length;
        } else {
          // found a space character. register the event but write it later
          spaces++;
          pos++;
        }
      }

      // write leftover spaces
      if (spaces > 0) {
        if (spaces == 1) {
          out.write(' ');
        } else {
          out.write(StringUtils.repeat("\\u0020", spaces));
        }
      }

      return out.toString();
    } catch (final IOException ioe) {
      // this should never ever happen while writing to a StringWriter
      throw new RuntimeException(ioe);
    }
  }

  private static class SIARDUnicodeEscaper extends UnicodeEscaper {
    public SIARDUnicodeEscaper() {
      super(0, 0, false);
    }

    /**
     * {@inheritDoc}
     *
     * @param codepoint
     * @param out
     */
    @Override
    public boolean translate(int codepoint, Writer out) throws IOException {
      // hardcoded characters to escape
      if (codepoint == 0x5C || codepoint == 0xB || codepoint == 0xC || codepoint == 0xE || codepoint == 0xF

        || (codepoint >= 0x0 && codepoint <= 0x8) || (codepoint >= 0x10 && codepoint < 0x20)

        || (codepoint >= 0x1A && codepoint <= 0x1F) || (codepoint >= 0x7F && codepoint <= 0x9F)

        || codepoint == 0xFFFE || codepoint == 0xFFFF) {

        if (codepoint > 0xffff) {
          out.write(toUtf16Escape(codepoint));
        } else if (codepoint > 0xfff) {
          out.write("\\u" + hex(codepoint));
        } else if (codepoint > 0xff) {
          out.write("\\u0" + hex(codepoint));
        } else if (codepoint > 0xf) {
          out.write("\\u00" + hex(codepoint));
        } else {
          out.write("\\u000" + hex(codepoint));
        }
        return true;
      } else {
        return false;
      }
    }
  }

  public static Object getXPathResult(final InputStream inputStream, final String xpathExpression, QName constants,
    final String type) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
    Document document = getDocument(inputStream);
    XPathFactory xPathFactory = XPathFactory.newInstance();
    XPath xpath = xPathFactory.newXPath();
    xpath = setXPath(xpath, type);
    XPathExpression expression = xpath.compile(xpathExpression);
    inputStream.close();

    return expression.evaluate(document, constants);
  }

  private static Document getDocument(InputStream inputStream)
      throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();

    return builder.parse(inputStream);
  }

  private static XPath setXPath(XPath xPath, final String type) {
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
          if (Constants.NAMESPACE_FOR_TABLE.equals(type)) {
            return "http://www.bar.admin.ch/xmlns/siard/2/table.xsd";
          }
          return "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd";
        }
        return null;
      }
    });

    return xPath;
  }

  public static Document convertStringToDocument(String xmlStreamReader, DocumentBuilder builder)
    throws IOException, SAXException {

    return builder.parse(new InputSource(new StringReader(xmlStreamReader)));
  }

  public static Element getChild(Element parent, String name) {
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element && name.equals(child.getNodeName())) {
        return (Element) child;
      }
    }
    return null;
  }

  public static String getChildTextContext(Element parent, String name) {
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element && name.equals(child.getNodeName())) {
        return child.getTextContent();
      }
    }
    return null;
  }

  public static String getParentNameByTagName(Element child, String tagName) {
    Element parent = (Element) child.getParentNode();
    if (parent == null)
      return null;

    if (parent.getNodeName().equals(tagName)) {
      return getChildTextContext(parent, "name");
    }

    return getParentNameByTagName(parent, tagName);
  }
}
