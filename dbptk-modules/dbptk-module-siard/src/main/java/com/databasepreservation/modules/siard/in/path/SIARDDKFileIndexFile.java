package com.databasepreservation.modules.siard.in.path;

import java.io.Serializable;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */

public class SIARDDKFileIndexFile implements Serializable {
  String folderName;
  String fileName;
  byte[] md5;

  public SIARDDKFileIndexFile() {

  }

  public String getFolderName() {
    return folderName;
  }

  public void setFolderName(String folderName) {
    this.folderName = folderName;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public byte[] getMd5() {
    return md5;
  }

  public void setMd5(byte[] md5) {
    this.md5 = md5;
  }

}
