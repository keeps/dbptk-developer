/**
 *
 */
package com.databasepreservation.model.data;

import java.io.InputStream;
import java.sql.Blob;

import com.databasepreservation.common.ProvidesInputStream;
import com.databasepreservation.model.exception.ModuleException;

/**
 * Represents a cell of BLOB type
 *
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class BinaryCell extends Cell implements ProvidesInputStream {
  private ProvidesInputStream providesInputStream;

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
    providesInputStream = new ProvidesBlobInputStream(blob);
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
    providesInputStream = new ProvidesTempFileInputStream(inputStream);
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    return providesInputStream.createInputStream();
  }

  @Override
  public void cleanResources() {
    providesInputStream.cleanResources();
  }

  @Override
  public long getSize() throws ModuleException {
    return providesInputStream.getSize();
  }
}
