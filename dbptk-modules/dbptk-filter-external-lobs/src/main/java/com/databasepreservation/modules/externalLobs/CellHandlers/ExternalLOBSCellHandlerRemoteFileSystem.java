
/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.io.InputStream;
import java.nio.file.Path;

import com.databasepreservation.managers.RemoteConnectionManager;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class ExternalLOBSCellHandlerRemoteFileSystem implements ExternalLOBSCellHandler {
  private final Path basePath;
  private Reporter reporter;

  public ExternalLOBSCellHandlerRemoteFileSystem() {
    basePath = null;
  }

  public ExternalLOBSCellHandlerRemoteFileSystem(Path basePath, Reporter reporter) {
    this.basePath = basePath;
    this.reporter = reporter;
  }

  @Override
  public Cell handleCell(String cellId, String cellValue) throws ModuleException {
    Cell newCell = new NullCell(cellId);

    if (basePath == null) {
      reporter.failed("Cell " + cellId, "Base path is not set");
      return newCell;
    }

    Path blobPath = basePath.resolve(cellValue);

    try {
      final InputStream stream = RemoteConnectionManager.getInstance().getInputStream(blobPath);
      newCell = new BinaryCell(cellId, stream);
    } catch (JSchException | SftpException e) {
      reporter.ignored("Cell " + cellId, "there was an error accessing the remote file " + blobPath.toString() + ": "
        + e.getMessage() + "; Base path: " + this.basePath + " Cell Value: " + cellValue);
    }

    return newCell;
  }
}
