package com.databasepreservation.modules.siard.common;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ProvidesInputStream {
  InputStream createInputStream() throws ModuleException;

  /**
   * Close the stream and free all underlying resources.
   */
  void cleanResources();
}
