package com.databasepreservation.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.translate.AggregateTranslator;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.commons.lang3.text.translate.UnicodeEscaper;
import org.apache.commons.lang3.text.translate.UnicodeUnescaper;

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
}
