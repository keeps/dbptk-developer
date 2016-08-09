package com.databasepreservation.modules.siard.common;

import java.io.InputStream;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ProvidesInputStreamFromBinaryCell implements ProvidesInputStream {
  private final BinaryCell binaryCell;

  public ProvidesInputStreamFromBinaryCell(BinaryCell binaryCell) {
    this.binaryCell = binaryCell;
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    return binaryCell.createInputstream();
  }

  /**
   * Close the stream and free all underlying resources.
   */
  @Override
  public void cleanResources() {
    binaryCell.cleanResourcesSilently();
  }
}
