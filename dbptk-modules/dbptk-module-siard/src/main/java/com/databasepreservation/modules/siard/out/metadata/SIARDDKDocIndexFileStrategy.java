/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.math.BigInteger;
import java.util.List;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKDocIndexFileStrategy<T, D> implements IndexFileStrategy {

  private T docIndex;

  public SIARDDKDocIndexFileStrategy() {
    docIndex = createDocIndexTypeInstance();
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
  public D addDoc(int dID, int pID, int mID, int docCollectionNumber, String oFn, String aFt, String gmlXsd) {
    D doc = createDocumentTypeInstance();

    setDID(doc, BigInteger.valueOf(dID));
    if (pID > 0) {
      // TO-DO: set pID
    }
    setMID(doc, BigInteger.valueOf(mID));
    setDCf(doc, "docCollection" + docCollectionNumber);
    setOFn(doc, oFn);
    setAFt(doc, aFt);
    if (gmlXsd != null) {
      setGmlXsd(doc, gmlXsd);
    }

    getDoc(docIndex).add(doc);

    return doc;
  }

  abstract T createDocIndexTypeInstance();

  abstract D createDocumentTypeInstance();

  abstract void setDID(D doc, BigInteger value);

  abstract void setMID(D doc, BigInteger value);

  abstract void setDCf(D doc, String dCf);

  abstract void setOFn(D doc, String oFn);

  abstract void setAFt(D doc, String aFt);

  abstract void setGmlXsd(D doc, String gmlXsd);

  abstract List<D> getDoc(T docIndex);
}

