/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.metadata;

import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public abstract class SIARDDKDocIndexMetadataImportHandler extends DefaultHandler {
  private int rows;

  public SIARDDKDocIndexMetadataImportHandler() {
    rows = 0;
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (localName.equals(getDocLocalName())) {
      endElementDoc();
    }
  }

  protected void endElementDoc() {
    rows++;
  }

  abstract String getDocLocalName();

  public int getRows() {
    return this.rows;
  }
}
