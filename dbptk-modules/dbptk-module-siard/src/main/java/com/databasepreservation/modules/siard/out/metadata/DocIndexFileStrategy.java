/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.math.BigInteger;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;

import dk.sa.xmlns.diark._1_0.docindex.DocIndexType;
import dk.sa.xmlns.diark._1_0.docindex.DocumentType;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class DocIndexFileStrategy implements IndexFileStrategy {

  private DocIndexType docIndex;

  public DocIndexFileStrategy() {
    docIndex = new DocIndexType();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.databasepreservation.modules.siard.out.metadata.IndexFileStrategy#
   * generateXML(com.databasepreservation.model.structure.DatabaseStructure)
   */
  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {
    return docIndex;
  }

  /**
   * Adds a doc element to the docIndex
   * 
   * @param dID
   *          value of the dID
   * @param pID
   *          value of the pID (only set if pID > 0)
   * @param mID
   *          value of the mID
   * @param docCollectionNumber
   *          value to append to "docCollection"
   * @param oFn
   *          original filename
   * @param aFt
   *          the filename extension
   * @param gmlXsd
   *          ...
   * @return the doc element containing the given data
   */
  public DocumentType addDoc(int dID, int pID, int mID, int docCollectionNumber, String oFn, String aFt,
    String gmlXsd) {
    DocumentType doc = new DocumentType();

    doc.setDID(BigInteger.valueOf(dID));
    if (pID > 0) {
      // TO-DO: set pID
    }
    doc.setMID(BigInteger.valueOf(mID));
    doc.setDCf("docCollection" + docCollectionNumber);
    doc.setOFn(oFn);
    doc.setAFt(aFt);
    if (gmlXsd != null) {
      doc.setGmlXsd(gmlXsd);
    }

    docIndex.getDoc().add(doc);

    return doc;
  }

}
