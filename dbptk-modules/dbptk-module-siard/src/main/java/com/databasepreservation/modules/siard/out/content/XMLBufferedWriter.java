/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class XMLBufferedWriter implements Appendable, Closeable, Flushable {
  private final static String INDENT = "    ";
  private final static char SPACE = ' ';
  private final static String NEWLINE = System.getProperty("line.separator");

  private final static char OPEN_TAG_BEGIN = '<';
  private final static char OPEN_TAG_END = '>';

  private final static String CLOSE_TAG_BEGIN = "</";
  private final static char CLOSE_TAG_END = OPEN_TAG_END;

  private final static char SHORT_TAG_BEGIN = OPEN_TAG_BEGIN;
  private final static String SHORT_TAG_END = "/>";

  private final static char ATTRIBUTE_ASSIGN = '=';
  private final static char ATTRIBUTE_VALUE_BEGIN = '"';
  private final static char ATTRIBUTE_VALUE_END = ATTRIBUTE_VALUE_BEGIN;

  private final BufferedWriter writer;
  private final boolean pretty;

  public XMLBufferedWriter(OutputStream out, boolean prettyPrint) {
    writer = new BufferedWriter(new OutputStreamWriter(out));
    pretty = prettyPrint;
  }

  // ////////////////////////////////////////////
  // Utility Methods
  // ////////////////////////////////////////////

  /**
   * " " (a single space) + key="value"
   */
  public XMLBufferedWriter appendAttribute(String key, String value) throws IOException {
    return space().append(key).append(ATTRIBUTE_ASSIGN).append(ATTRIBUTE_VALUE_BEGIN).append(value)
      .append(ATTRIBUTE_VALUE_END);
  }

  /**
   * indentation (if pretty) + "&lt;tagName"
   */
  public XMLBufferedWriter beginOpenTag(String tagName, int levelsToIndent) throws IOException {
    return indent(levelsToIndent).append(OPEN_TAG_BEGIN).append(tagName);
  }

  /**
   * "&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter endOpenTag() throws IOException {
    return append(OPEN_TAG_END).newline();
  }

  /**
   * "/&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter endShorthandTag() throws IOException {
    return append(SHORT_TAG_END).newline();
  }

  /**
   * indentation (if pretty) + "&lt;/tagName&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter closeTag(String tagName, int levelsToIndent) throws IOException {
    return indent(levelsToIndent).append(CLOSE_TAG_BEGIN).append(tagName).append(CLOSE_TAG_END).newline();
  }

  /**
   * "&lt;/tagName&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter closeTag(String tagName) throws IOException {
    return closeTag(tagName, 0);
  }

  /**
   * indentation (if pretty) + "&lt;tagName&gt;"
   */
  public XMLBufferedWriter inlineOpenTag(String tagName, int levelsToIndent) throws IOException {
    return beginOpenTag(tagName, levelsToIndent).append(OPEN_TAG_END);
  }

  /**
   * indentation (if pretty) + "&lt;tagName&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter openTag(String tagName, int levelsToIndent) throws IOException {
    return inlineOpenTag(tagName, levelsToIndent).newline();
  }

  /**
   * indentation (if pretty) + "&lt;tagContent/&gt;" + newline (if pretty)
   */
  public XMLBufferedWriter shorthandTag(String tagContent, int levelsToIndent) throws IOException {
    indent(levelsToIndent).append(SHORT_TAG_BEGIN).append(tagContent).append(SHORT_TAG_END).newline();
    return this;
  }

  /**
   * indentation (if pretty)
   */
  public XMLBufferedWriter indent(int levelsToIndent) throws IOException {
    if (pretty) {
      for (int i = 0; i < levelsToIndent; i++) {
        write(INDENT);
      }
    }
    return this;
  }

  /**
   * newline (if pretty)
   */
  public XMLBufferedWriter newline() throws IOException {
    if (pretty) {
      write(NEWLINE);
    }
    return this;
  }

  /**
   * " " (a single space)
   */
  public XMLBufferedWriter space() throws IOException {
    return append(SPACE);
  }

  // ////////////////////////////////////////////
  // Delegated Methods (except for append)
  // ////////////////////////////////////////////

  public void write(int c) throws IOException {
    writer.write(c);
  }

  public void write(char[] cbuf) throws IOException {
    writer.write(cbuf);
  }

  public void write(char[] cbuf, int off, int len) throws IOException {
    writer.write(cbuf, off, len);
  }

  public void write(String str) throws IOException {
    writer.write(str);
  }

  public void write(String str, int off, int len) throws IOException {
    writer.write(str, off, len);
  }

  @Override
  public XMLBufferedWriter append(CharSequence csq) throws IOException {
    writer.append(csq);
    return this;
  }

  @Override
  public XMLBufferedWriter append(CharSequence csq, int start, int end) throws IOException {
    writer.append(csq, start, end);
    return this;
  }

  @Override
  public XMLBufferedWriter append(char c) throws IOException {
    writer.append(c);
    return this;
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
