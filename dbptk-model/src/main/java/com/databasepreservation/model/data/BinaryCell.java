/**
 *
 */
package com.databasepreservation.model.data;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import pt.gov.dgarq.roda.common.FileFormat;

import com.databasepreservation.model.exception.ModuleException;

/**
 * Represents the value of a cell of BLOB type
 *
 * @author Luis Faria
 */
public class BinaryCell extends Cell {

  private FileItem fileItem;

  private List<FileFormat> formatHits;

  /**
   * Binary cell constructor without a FileItem. This should not be used to represent NULL, instead a NullCell should be created.
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   */
  protected BinaryCell(String id) {
    super(id);
    fileItem = null;
    formatHits = new ArrayList<FileFormat>();
  }

  /**
   * Binary cell constructor
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   * @param fileItem
   *          the fileItem relative to the binary data
   * @throws ModuleException
   */
  public BinaryCell(String id, FileItem fileItem) throws ModuleException {
    super(id);
    this.fileItem = fileItem;
    this.formatHits = new ArrayList<FileFormat>();
  }

  /**
   * Binary cell constructor, with optional mimetype attribute
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   * @param fileItem
   *          the fileItem relative to the binary data
   * @param formatHits
   *          the possible formats of this binary
   * @throws ModuleException
   */
  public BinaryCell(String id, FileItem fileItem, List<FileFormat> formatHits) throws ModuleException {
    super(id);
    this.fileItem = fileItem;
    this.formatHits = formatHits;
  }

  /**
   * @return create an inputstream to fetch the binary data
   * @throws ModuleException
   */
  public InputStream createInputstream() throws ModuleException {
    return fileItem != null ? fileItem.createInputStream() : null;
  }

  /**
   * @param inputstream
   *          the inputstream to fetch the binary data
   * @throws ModuleException
   */
  public void setInputstream(InputStream inputstream) throws ModuleException {
    this.fileItem = new FileItem(inputstream);
  }

  /**
   * @return the possible formats of this binary
   */
  public List<FileFormat> getFormatHits() {
    return formatHits;
  }

  /**
   * @param formatHits
   *          the possible formats of this binary
   */
  public void setFormatHits(List<FileFormat> formatHits) {
    this.formatHits = formatHits;
  }

  /**
   * Get the binary stream length in bytes
   *
   * @return the binary stream length
   */
  public long getLength() throws ModuleException {
    return fileItem != null ? fileItem.size() : 0;
  }

  /**
   * Clear resources used by binary cell
   *
   * @return true if successfuly cleared all resources
   */
  public boolean cleanResources() {
    return fileItem.delete();
  }

}
