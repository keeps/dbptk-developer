package com.databasepreservation.modules.siard.in.content;

import java.io.Serializable;
import java.math.BigInteger;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */

public class SIARDDKDocIndexDoc implements Serializable {
  BigInteger documentID;
  BigInteger parentID;
  BigInteger mediaID;
  String documentCollectionFolder;
  String originalFilename;
  String archivalFileType;
  String gmlXSD;

  public SIARDDKDocIndexDoc() {

  }

  public BigInteger getParentID() {
    return parentID;
  }

  public void setParentID(BigInteger parentID) {
    this.parentID = parentID;
  }

  public BigInteger getDocumentID() {
    return documentID;
  }

  public void setDocID(BigInteger documentID) {
    this.documentID = documentID;
  }

  public BigInteger getMediaID() {
    return mediaID;
  }

  public void setMediaID(BigInteger mediaID) {
    this.mediaID = mediaID;
  }

  public String getDocumentCollectionFolder() {
    return documentCollectionFolder;
  }

  public void setContainerFolder(String documentCollectionFolder) {
    this.documentCollectionFolder = documentCollectionFolder;
  }

  public String getOriginalFilename() {
    return originalFilename;
  }

  public void setOriginalFilename(String originalFilename) {
    this.originalFilename = originalFilename;
  }

  public String getArchivalFileType() {
    return archivalFileType;
  }

  public void setArchivalFileType(String archivalFileType) {
    this.archivalFileType = archivalFileType;
  }

  public String getGmlXSD() {
    return gmlXSD;
  }

  public void setGmlXSD(String gmlXSD) {
    this.gmlXSD = gmlXSD;
  }

}
