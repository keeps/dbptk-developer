/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.data;

import java.io.InputStream;
import java.sql.Blob;

import com.databasepreservation.common.io.providers.BlobInputStreamProvider;
import com.databasepreservation.common.io.providers.InputStreamProvider;
import com.databasepreservation.common.io.providers.TemporaryPathInputStreamProvider;
import com.databasepreservation.model.exception.ModuleException;

/**
 * Represents a cell of BLOB type
 *
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class BinaryCell extends Cell implements InputStreamProvider {
  private InputStreamProvider inputStreamProvider;
  private String file;
  private long length;

  /**
   * Creates a binary cell. This binary cell will mostly just be a wrapper around
   * the SQL Blob object.
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
   * Creates a binary cell. The binary contents are read and saved to a temporary
   * file, so they can be read later without keeping an open InputStreams.
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

  /**
   * Creates a binary cell. This binary cell is a wrapper around a
   * ProvidesInputStream object (whilst also providing Cell functionality).
   *
   * @param id
   *          the cell id
   * @param inputStreamProvider
   *          the inputStream provider used to read BLOB data
   * @param file
   *          the BLOB file name extract from table.xml
   * @param length
   *          the BLOB length extract from table.xml
   * @param digest
   *          the BLOB digest extract from table.xml
   * @param digestType
   *          the BLOB digestType extract from table.xml
   */
  public BinaryCell(String id, InputStreamProvider inputStreamProvider, String file, long length, String digest,
    String digestType) {
    super(id);
    this.inputStreamProvider = inputStreamProvider;
    this.file = file;
    this.length = length;
    setMessageDigest(digest.getBytes());
    setDigestAlgorithm(digestType);
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

  public InputStreamProvider getInputStreamProvider() {
    return inputStreamProvider;
  }

  public String getFile() {
    return file;
  }

  public long getLength() {
    return length;
  }
}
