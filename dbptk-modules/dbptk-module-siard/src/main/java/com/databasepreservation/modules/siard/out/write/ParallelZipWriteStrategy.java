/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.write;

import com.databasepreservation.common.compression.CompressionMethod;
import com.databasepreservation.common.io.providers.InputStreamProvider;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.zip.ZipEntry;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ParallelZipWriteStrategy implements WriteStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipWriteStrategy.class);

  private final CompressionMethod compressionMethod;
  private ProtectedZipArchiveOutputStream zipOut;
  private ParallelScatterZipCreator scatterZipCreator = new ParallelScatterZipCreator();

  public ParallelZipWriteStrategy(CompressionMethod compressionMethod) {
    this.compressionMethod = compressionMethod;
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
  public void writeTo(InputStreamProvider provider, String path) {
    ZipArchiveEntry entry = new ZipArchiveEntry(path);
    entry.setMethod(ZipEntry.DEFLATED);
    scatterZipCreator.addArchiveEntry(entry, provider);
  }

  @Override
  public boolean isSimultaneousWritingSupported() {
    return true;
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
      scatterZipCreator.writeTo(zipOut);
    } catch (IOException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
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
