/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.util.List;

import com.databasepreservation.modules.siard.bindings.siard_dk_128_ext.FileIndexType;
import com.databasepreservation.modules.siard.bindings.siard_dk_128_ext.FileIndexType.F;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK128ExtPathImportStrategy extends SIARDDKPathImportStrategy<F, FileIndexType> {

  public SIARDDK128ExtPathImportStrategy(SIARDArchiveContainer mainFolder, ReadStrategy readStrategy,
    MetadataPathStrategy metadataPathStrategy, String importAsSchema,
    FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy) {
    super(mainFolder, readStrategy, metadataPathStrategy, importAsSchema, fileIndexXsdInputStreamStrategy,
      FileIndexType.class);
  }

  @Override
  byte[] getMd5(F fileInfo) {
    return fileInfo.getMd5();
  }

  @Override
  List<F> getF(FileIndexType fileIndex) {
    return fileIndex.getF();
  }

  @Override
  String getFoN(F fileInfo) {
    return fileInfo.getFoN();
  }

  @Override
  String getFiN(F fileInfo) {
    return fileInfo.getFiN();
  }
}
