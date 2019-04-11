package com.databasepreservation.modules.externalLobs.CellHandlers;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ExternalLOBSCellHandlerFileSystem implements ExternalLOBSCellHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSCellHandlerFileSystem.class);
  private Path basePath;

  public ExternalLOBSCellHandlerFileSystem() {
    basePath = null;
  }

  public ExternalLOBSCellHandlerFileSystem(Path basePath) {
    this.basePath = basePath;
  }

  @Override
  public BinaryCell handleCell(String cellId, String cellValue) throws ModuleException {
    Path blobPath = basePath.resolve(cellValue);
    BinaryCell newCell = null;

    try (InputStream stream = Files.newInputStream(blobPath)) {
      newCell = new BinaryCell(cellId, stream);
    } catch (IOException e) {
      LOGGER.debug("Could not open stream to file", e);
    }
    return newCell;
  }

  @Override
  public String handleTypeDescription(String originalTypeDescription) {
    return "Converted to LOB referenced by file system path (original description: '"+originalTypeDescription+"')";
  }
}
