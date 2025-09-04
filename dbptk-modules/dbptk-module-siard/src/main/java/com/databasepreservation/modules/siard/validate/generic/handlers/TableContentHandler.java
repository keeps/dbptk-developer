/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.generic.handlers;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Set;

public class TableContentHandler extends DefaultHandler {
    private final String columnIndex;
    private boolean indexFound;
    private StringBuilder tmp = new StringBuilder();
    private Set<String> data;

    public TableContentHandler(final String columnIndex, final Set<String> data) {
        this.columnIndex = columnIndex;
        this.data = data;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        if (qName.equals(columnIndex)) {
            indexFound = true;
            tmp.setLength(0);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equals(columnIndex)) {
            data.add(tmp.toString());
            indexFound = false;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (indexFound) {
            tmp.append(ch, start, length);
        }
    }


}
