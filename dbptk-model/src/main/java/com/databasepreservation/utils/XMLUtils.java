package com.databasepreservation.utils;

import org.apache.commons.lang3.StringEscapeUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class XMLUtils {
        /**
         * Encodes a data string as defined by SIARD format
         *
         * @param text the text string to be encoded
         * @return the encoded (XML-safe) text
         */
        public static final String encode(String text) {
                String xml = text.replaceAll("\\\\", "\\u005C");
                xml = StringEscapeUtils.escapeJava(xml);
                xml = xml.replaceAll("&", "&amp;");
                xml = xml.replaceAll("<", "&lt;");
                xml = xml.replaceAll(">", "&gt;");
                xml = xml.replaceAll("\"", "&quot;");
                xml = xml.replaceAll("\\s+", "\\\\u0020");
                return xml;
        }

        /**
         * Decodes an encoded string as defined by SIARD format
         *
         * @param xml the encoded string
         * @return the original string
         */
        public static final String decode(String xml) {
                String text = xml.replace("\\\\u0020", " ");
                text = text.replaceAll("&quot;", "\"");
                text = text.replaceAll("&gt;", ">");
                text = text.replaceAll("&lt;", "<");
                text = text.replaceAll("&amp;", "&");
                text = StringEscapeUtils.unescapeJava(text);
                text = text.replaceAll("\\u005C", "\\\\");
                return text;
        }
}
