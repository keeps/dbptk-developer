/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common;

import java.io.InputStream;

import org.apache.commons.compress.parallel.InputStreamSupplier;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface InputStreamProvider extends InputStreamSupplier {
  /**
   * Create a new input stream to read data
   *
   * Contract: The stream must be closed elsewhere. It is not closed automatically
   * in any way, not even by cleanResources
   *
   * @return the new input stream
   * @throws ModuleException
   *           if the input stream could not be created
   */
  InputStream createInputStream() throws ModuleException;

  @Override
  default InputStream get() {
    try {
      return createInputStream();
    } catch (ModuleException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Free all underlying resources, except for the stream itself.
   */
  void cleanResources();

  /**
   * @return the total number of bytes that can be provided by the InputStream
   *         created with #createInputStream
   * @throws ModuleException
   *           if the size could not be obtained
   */
  long getSize() throws ModuleException;
}
