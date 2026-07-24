package com.databasepreservation.modules.siard.in.path;

import java.util.Map;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK128ExtFileIndexHandler extends SIARDDKFileIndexHandler {

  public SIARDDK128ExtFileIndexHandler(Map<String, String> archiveFolderLookupByFolderName,
    Map<String, SIARDDKFileIndexFile> xsdFilePathLookupByFolderName,
    Map<String, SIARDDKFileIndexFile> xmlFilePathLookupByFolderName) {
    super(archiveFolderLookupByFolderName, xsdFilePathLookupByFolderName, xmlFilePathLookupByFolderName);
  }

  @Override
  String getFileLocalName() {
    return "f";
  }

  @Override
  String getFileNameLocalName() {
    return "fiN";
  }

  @Override
  String getFolderNameLocalName() {
    return "foN";
  }

  @Override
  String getMD5LocalName() {
    return "md5";
  }
}
