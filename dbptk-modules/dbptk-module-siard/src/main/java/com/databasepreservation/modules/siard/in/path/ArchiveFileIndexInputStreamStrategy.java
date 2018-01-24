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
  public InputStream getInputStream(SIARDDKPathImportStrategy siarddkPathImportStrategy) throws ModuleException {
    ReadStrategy readStrategy = siarddkPathImportStrategy.getReadStrategy();
    SIARDArchiveContainer mainFolder = siarddkPathImportStrategy.getMainFolder();
    String xsdFilePath = siarddkPathImportStrategy.getXsdFilePath(SIARDDKConstants.FILE_INDEX);

    return readStrategy.createInputStream(mainFolder, xsdFilePath);
  }

}
