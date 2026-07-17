package com.databasepreservation.modules.siard.in.content;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK1007DocIndexHandler extends SIARDDKDocIndexHandler {

  public SIARDDK1007DocIndexHandler(SIARDDKPathImportStrategy pathImportStrategy,
                                    DatabaseExportModule databaseExportModule) {
    super(pathImportStrategy, databaseExportModule);
  }

  @Override
  String getDocLocalName() {
    return "doc";
  }

  @Override
  String getDocIDLocalName() {
    return "dID";
  }

  @Override
  String getParentIDLocalName() {
    return "pID";
  }

  @Override
  String getMediaIDLocalName() {
    return "mID";
  }

  @Override
  String getContainerFolderLocalName() {
    return "dCf";
  }

  @Override
  String getOriginalFilenameLocalName() {
    return "oFn";
  }

  @Override
  String getArchivalFileTypeLocalName() {
    return "aFt";
  }

  @Override
  String getGmlXSDLocalName() {
    return "gmlXsd";
  }
}
