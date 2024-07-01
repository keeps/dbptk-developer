/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
class ArchiveFileIndexInputStreamStrategy implements FileIndexXsdInputStreamStrategy {

  /*
   * (non-Javadoc)
   * 
   * @see com.databasepreservation.modules.siard.in.path.
   * FileIndexXsdInputStreamStrategy
   * #getInputStream(com.databasepreservation.modules.siard.in.path.
   * SIARDDKPathImportStrategy)
   */
  @Override
  public InputStream getInputStream(SIARDDK2010PathImportStrategy siarddk2010PathImportStrategy) throws ModuleException {
    ReadStrategy readStrategy = siarddk2010PathImportStrategy.getReadStrategy();
    SIARDArchiveContainer mainFolder = siarddk2010PathImportStrategy.getMainFolder();
    String xsdFilePath = siarddk2010PathImportStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX);

    return readStrategy.createInputStream(mainFolder, xsdFilePath);
  }

  @Override
  public InputStream getInputStream(SIARDDK2020PathImportStrategy siarddk2020PathImportStrategy) throws ModuleException {
    ReadStrategy readStrategy = siarddk2020PathImportStrategy.getReadStrategy();
    SIARDArchiveContainer mainFolder = siarddk2020PathImportStrategy.getMainFolder();
    String xsdFilePath = siarddk2020PathImportStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX);

    return readStrategy.createInputStream(mainFolder, xsdFilePath);
  }

}
