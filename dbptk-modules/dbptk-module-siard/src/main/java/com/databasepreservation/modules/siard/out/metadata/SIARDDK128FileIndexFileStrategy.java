/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 * The methods should be called in this order from the SIARDDKMetadataExportStrategy
 * 1) getWriter
 * 2) addFile (should not be called until writer obtained from getWriter is closed)
 * 3) generateXML
 * 
 *  TO-DO:
 *  NOTE: this class should be rewritten: the getLOBwriter part should be made smarter (more generic)
 */
package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.modules.siard.bindings.siard_dk_128.FileIndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK128FileIndexFileStrategy extends SIARDDKFileIndexFileStrategy<FileIndexType, FileIndexType.F> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDK128FileIndexFileStrategy.class);

  public SIARDDK128FileIndexFileStrategy() {
    super();
  }

  @Override
  FileIndexType createFileIndexTypeInstance() {
    return new FileIndexType();
  }

  @Override
  FileIndexType.F createFileIndexTypeFInstance() {
    return new FileIndexType.F();
  }

  @Override
  List<FileIndexType.F> getF(FileIndexType fileIndexType) {
    return fileIndexType.getF();
  }

  @Override
  void setFoN(FileIndexType.F file, String foN) {
    file.setFoN(foN);
  }

  @Override
  void setFiN(FileIndexType.F file, String fiN) {
    file.setFiN(fiN);
  }

  @Override
  void setMd5(FileIndexType.F file, byte[] value) {
    file.setMd5(value);
  }
}
