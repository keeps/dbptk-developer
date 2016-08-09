package com.databasepreservation.modules.siard.common;

import java.io.InputStream;

import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ProvidesInputStreamFromFileItem implements ProvidesInputStream {
  private final FileItem fileItem;

  public ProvidesInputStreamFromFileItem(FileItem fileItem) {
    this.fileItem = fileItem;
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    return fileItem.createInputStream();
  }

  /**
   * Close the stream and free all underlying resources.
   */
  @Override
  public void cleanResources() {
    fileItem.deleteSilently();
  }
}
