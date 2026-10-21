/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.util.Map;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK1007FileIndexHandler extends SIARDDKFileIndexHandler {

  public SIARDDK1007FileIndexHandler(Map<String, String> archiveFolderLookupByFolderName,
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
