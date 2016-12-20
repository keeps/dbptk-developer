/**
 *
 */
package com.databasepreservation.model.data;

import java.io.InputStream;
import java.sql.Blob;

import com.databasepreservation.common.BlobInputStreamProvider;
import com.databasepreservation.common.InputStreamProvider;
import com.databasepreservation.common.TemporaryPathInputStreamProvider;
import com.databasepreservation.model.exception.ModuleException;

/**
 * Represents a cell of BLOB type
 *
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class BinaryCell extends Cell implements InputStreamProvider {
  private InputStreamProvider inputStreamProvider;

  /**
   * Creates a binary cell. This binary cell will mostly just be a wrapper
   * around the SQL Blob object.
   *
   * @param id
   *          the cell id
   * @param blob
   *          the SQL Blob object, where the blob value will be read from
   */
  public BinaryCell(String id, Blob blob) {
    super(id);
    inputStreamProvider = new BlobInputStreamProvider(blob);
  }

  /**
   * Creates a binary cell. The binary contents are read and saved to a
   * temporary file, so they can be read later without keeping an open
   * InputStreams.
   *
   * The inputStream is closed after use.
   * 
   * @param id
   *          the cell id
   * @param inputStream
   *          to read the data. It will be closed.
   * @throws ModuleException
   *           if some IO problem occurs. The stream will still be closed.
   */
  public BinaryCell(String id, InputStream inputStream) throws ModuleException {
    super(id);
    inputStreamProvider = new TemporaryPathInputStreamProvider(inputStream);
  }

  /**
   * Creates a binary cell. This binary cell is a wrapper around a
   * ProvidesInputStream object (whilst also providing Cell functionality).
   *
   * @param id
   *          the cell id
   * @param inputStreamProvider
   *          the inputStream provider used to read BLOB data
   */
  public BinaryCell(String id, InputStreamProvider inputStreamProvider) {
    super(id);
    this.inputStreamProvider = inputStreamProvider;
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    return inputStreamProvider.createInputStream();
  }

  @Override
  public void cleanResources() {
    inputStreamProvider.cleanResources();
  }

  @Override
  public long getSize() throws ModuleException {
    return inputStreamProvider.getSize();
  }
}
