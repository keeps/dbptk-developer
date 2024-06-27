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
  public InputStream getInputStream(SIARDDKPathImportStrategy<?, ?> strategy) throws ModuleException {
    if (strategy instanceof SIARDDK1007PathImportStrategy) {
      return this.getClass().getClassLoader().getResourceAsStream("schema/1007/fileIndex.xsd");
    } else if (strategy instanceof SIARDDK128PathImportStrategy) {
      return this.getClass().getClassLoader().getResourceAsStream("schema/128/fileIndex.xsd");
    } else {
      throw new ModuleException();
    }
  }
}
