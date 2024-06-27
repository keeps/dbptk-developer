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

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class ResourceFileIndexInputStreamStrategy implements FileIndexXsdInputStreamStrategy {

  /*
   * (non-Javadoc)
   * 
   * @see com.databasepreservation.modules.siard.in.path.
   * FileIndexXsdInputStreamStrategy
   * #getInputStream(com.databasepreservation.modules.siard.in.path.
   * SIARDDKPathImportStrategy)
   */
  @Override
  public InputStream getInputStream(SIARDDK1007PathImportStrategy siarddk1007PathImportStrategy) throws ModuleException {
    return this.getClass().getClassLoader().getResourceAsStream("schema/1007/fileIndex.xsd");
  }

  @Override
  public InputStream getInputStream(SIARDDK128PathImportStrategy siarddk128PathImportStrategy) throws ModuleException {
    return this.getClass().getClassLoader().getResourceAsStream("schema/128/fileIndex.xsd");
  }

}
