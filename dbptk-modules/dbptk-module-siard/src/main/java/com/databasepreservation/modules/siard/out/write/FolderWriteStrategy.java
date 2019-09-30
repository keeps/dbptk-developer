/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.write;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class FolderWriteStrategy implements WriteStrategy {
  @Override
  public OutputStream createOutputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    Path filepath = container.getPath().resolve(path);

    if (!Files.exists(filepath)) {
      try {
        if (!Files.exists(filepath.getParent())) {
          Files.createDirectories(filepath.getParent());
        }
        Files.createFile(filepath);
      } catch (IOException e) {
        throw new ModuleException().withMessage("Error while creating the file: " + filepath.toString()).withCause(e);
      }
    }

    try {
      return Files.newOutputStream(filepath);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error while getting the file: " + filepath.toString()).withCause(e);
    }
  }

  @Override
  public boolean isSimultaneousWritingSupported() {
    return true;
  }

  @Override
  public void finish(SIARDArchiveContainer baseContainer) throws ModuleException {
    // nothing to do
  }

  @Override
  public void setup(SIARDArchiveContainer baseContainer) throws ModuleException {
    // nothing to do
  }

  @Override
  public DigestAlgorithm getDigestAlgorithm() {
    return DigestAlgorithm.NONE;
  }
}
