/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.write;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipWriteStrategy implements WriteStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipWriteStrategy.class);

  private final CompressionMethod compressionMethod;
  private ProtectedZipArchiveOutputStream zipOut;
  private DigestAlgorithm digestAlgorithm;

  public ZipWriteStrategy(CompressionMethod compressionMethod) {
    this.compressionMethod = compressionMethod;
    this.digestAlgorithm = DigestAlgorithm.MD5;
  }

  @Override
  public OutputStream createOutputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    ArchiveEntry archiveEntry = new ZipArchiveEntry(path);
    try {
      zipOut.putArchiveEntry(archiveEntry);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error creating new entry in zip file").withCause(e);
    }
    return zipOut;
  }

  @Override
  public boolean isSimultaneousWritingSupported() {
    return false;
  }

  @Override
  public void finish(SIARDArchiveContainer container) throws ModuleException {
    try {
      zipOut.closeArchiveEntry();
    } catch (IOException e) {
      if (!"No current entry to close".equals(e.getMessage())) {
        LOGGER.debug("the ArchiveEntry is already closed or the ZipArchiveOutputStream is already finished", e);
      }
    }

    try {
      zipOut.finish();
      zipOut.protectedClose();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Problem while finalizing zip output stream").withCause(e);
    }
  }

  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    try {
      zipOut = new ProtectedZipArchiveOutputStream(container.getPath().toFile());

      zipOut.setUseZip64(Zip64Mode.AsNeeded);

      switch (compressionMethod) {
        case DEFLATE:
          zipOut.setMethod(ZipArchiveOutputStream.DEFLATED);
          break;
        case STORE:
          zipOut.setMethod(ZipArchiveOutputStream.STORED);
          break;
        default:
          throw new ModuleException().withMessage("Invalid compression method: " + compressionMethod);
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error creating SIARD archive file: " + compressionMethod).withCause(e);
    }
  }

  public enum CompressionMethod {
    DEFLATE, STORE
  }

  public DigestAlgorithm getDigestAlgorithm() {
    return this.digestAlgorithm;
  }

  private class ProtectedZipArchiveOutputStream extends ZipArchiveOutputStream {
    public ProtectedZipArchiveOutputStream(File file) throws IOException {
      super(file);
    }

    @Override
    public void close() throws IOException {
      flush();
      zipOut.closeArchiveEntry();
    }

    private void protectedClose() throws IOException {
      super.close();
    }
  }
}
