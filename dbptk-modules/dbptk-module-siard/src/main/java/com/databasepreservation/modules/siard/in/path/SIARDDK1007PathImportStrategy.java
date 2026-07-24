/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.path;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.siard.bindings.siard_dk_1007.FileIndexType;
import com.databasepreservation.modules.siard.bindings.siard_dk_1007.FileIndexType.F;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK1007PathImportStrategy extends SIARDDKPathImportStrategy<FileIndexType.F, FileIndexType> {
  protected final Logger logger = LoggerFactory.getLogger(ContentPathImportStrategy.class);

  public SIARDDK1007PathImportStrategy(SIARDArchiveContainer mainFolder, ReadStrategy readStrategy,
    MetadataPathStrategy metadataPathStrategy, String importAsSchema,
    FileIndexXsdInputStreamStrategy fileIndexXsdInputStreamStrategy) {
    super(mainFolder, readStrategy, metadataPathStrategy, importAsSchema, fileIndexXsdInputStreamStrategy,
      FileIndexType.class);
  }

  @Override
  SIARDDKFileIndexHandler createFileIndexHandler() {
    return null;
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
