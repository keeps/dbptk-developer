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

import dk.sa.xmlns.diark._1_0.docindex.DocIndexType;
import dk.sa.xmlns.diark._1_0.docindex.DocumentType;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK1007DocIndexFileStrategy extends SIARDDKDocIndexFileStrategy<DocIndexType, DocumentType> {

  public SIARDDK1007DocIndexFileStrategy() {
    super();
  }

  @Override
  DocIndexType createDocIndexTypeInstance() {
    return new DocIndexType();
  }

  @Override
  DocumentType createDocumentTypeInstance() {
    return new DocumentType();
  }

  @Override
  void setDID(DocumentType doc, BigInteger value) {
    doc.setDID(value);
  }

  @Override
  void setMID(DocumentType doc, BigInteger value) {
    doc.setMID(value);
  }

  @Override
  void setDCf(DocumentType doc, String dCf) {
    doc.setDCf(dCf);
  }

  @Override
  void setOFn(DocumentType doc, String oFn) {
    doc.setOFn(oFn);
  }

  @Override
  void setAFt(DocumentType doc, String aFt) {
    doc.setAFt(aFt);
  }

  @Override
  void setGmlXsd(DocumentType doc, String gmlXsd) {
    doc.setGmlXsd(gmlXsd);
  }

  @Override
  List<DocumentType> getDoc(DocIndexType docIndex) {
    return docIndex.getDoc();
  }

}
