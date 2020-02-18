/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common.providers;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class InputStreamProviderImpl implements InputStreamProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(InputStreamProviderImpl.class);

  private InputStream inputStream;
  private long size;

  public InputStreamProviderImpl(InputStream inputStream) {
    this.inputStream = inputStream;
  }


  public InputStreamProviderImpl(InputStream inputStream, long size) {
    this.inputStream = inputStream;
    this.size = size;
  }

  /**
   * Create a new input stream to read data
   * <p>
   * Contract: The stream must be closed elsewhere. It is not closed automatically
   * in any way, not even by cleanResources
   *
   * @return the new input stream
   * @throws ModuleException
   *           if the input stream could not be created
   */
  @Override
  public InputStream createInputStream() throws ModuleException {
    return inputStream;
  }

  /**
   * Free all underlying resources, except for the stream itself.
   */
  @Override
  public void cleanResources() {
    try {
      inputStream.close();
    } catch (IOException e) {
      LOGGER.debug("Could not close the stream", e);
    }
  }

  /**
   * @return the total number of bytes that can be provided by the InputStream
   *         created with #createInputStream
   * @throws ModuleException
   *           if the size could not be obtained
   */
  @Override
  public long getSize() throws ModuleException {
    return size;
  }
}
