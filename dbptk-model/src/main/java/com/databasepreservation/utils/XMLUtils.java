package com.databasepreservation.utils;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class XMLUtils {
  /**
   * Encodes a data string as defined by SIARD formats
   *
   * @param text
   *          the text string to be encoded
   * @return the encoded (XML-safe) text
   */
  public static final String encode(String text) {
    text = text.replace("\\", "\\u005c");

    for (int charCode = 0; charCode <= 8; charCode++) {
      text = text.replace(String.valueOf(Character.toChars(charCode)), "\\u000" + charCode);
    }

    for (int charCode = 14; charCode <= 15; charCode++) {
      text = text.replace(String.valueOf(Character.toChars(charCode)), "\\u000" + Integer.toHexString(charCode));
    }

    for (int charCode = 16; charCode <= 32; charCode++) {
      text = text.replace(String.valueOf(Character.toChars(charCode)), "\\u00" + Integer.toHexString(charCode));
    }

    for (int charCode = 127; charCode <= 159; charCode++) {
      text = text.replace(String.valueOf(Character.toChars(charCode)), "\\u00" + Integer.toHexString(charCode));
    }

    text = text.replace("\"", "&quot;");
    text = text.replace("&", "&amp;");
    text = text.replace("'", "&apos;");
    text = text.replace("<", "&lt;");
    text = text.replace(">", "&gt;");

    // all ' ' (space character) were replaced by .
    // single spaces must be converted back to a ' ' character.
    // multiple spaces must continue as sequences of "\u0020"
    text = text.replaceAll("(?<!\\\\u0020)\\\\u0020(?!\\\\u0020)", " ");

    return text;
  }

  /**
   * Decodes an encoded string as defined by SIARD formats
   *
   * @param text
   *          the encoded string
   * @return the original string
   */
  public static final String decode(String text) {
    text = text.replace("&gt;", ">");
    text = text.replace("&lt;", "<");
    text = text.replace("&apos;", "'");
    text = text.replace("&amp;", "&");
    text = text.replace("&quot;", "\"");

    text = StringEscapeUtils.unescapeJava(text);

    return text;
  }
}
