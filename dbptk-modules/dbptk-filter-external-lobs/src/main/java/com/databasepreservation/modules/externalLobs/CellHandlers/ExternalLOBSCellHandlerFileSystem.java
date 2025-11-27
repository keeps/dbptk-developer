/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.nio.file.Files;
import java.nio.file.Path;

import com.databasepreservation.common.io.providers.PathInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;

public class ExternalLOBSCellHandlerFileSystem implements ExternalLOBSCellHandler {
  private final Path basePath;
  private Reporter reporter;

  public ExternalLOBSCellHandlerFileSystem() {
    basePath = null;
  }

  public ExternalLOBSCellHandlerFileSystem(Path basePath, Reporter reporter) {
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

    if (Files.exists(blobPath)) {
      if (Files.isRegularFile(blobPath)) {
          try {
              newCell = new BinaryCell(cellId, new PathInputStreamProvider(blobPath));
          } catch (ModuleException e) {
              reporter.ignored("Cell " + cellId,
                      blobPath.toString() + " ignore due to: " + e.getMessage() + "; Base path: " + this.basePath + " Cell Value: " + cellValue);
          }
      } else {
        reporter.ignored("Cell " + cellId,
          blobPath.toString() + " is not a file; Base path: " + this.basePath + " Cell Value: " + cellValue);
      }
    } else {
      reporter.ignored("Cell " + cellId, "Path: " + blobPath.toString() + " could not be found; Base path: "
        + this.basePath + " Cell Value: " + cellValue);
    }
    return newCell;
  }
}
