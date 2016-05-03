/**
 *
 */
package com.databasepreservation.model.data;

import java.io.IOException;
import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

/**
 * Represents the value of a cell of BLOB type
 *
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class BinaryCell extends Cell {

  private FileItem fileItem;

  /**
   * Binary cell constructor without a FileItem. This should not be used to
   * represent NULL, instead a NullCell should be created.
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   */
  protected BinaryCell(String id) {
    super(id);
    fileItem = null;
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
  }

  /**
   * @return create an inputstream to fetch the binary data
   * @throws ModuleException
   */
  public InputStream createInputstream() throws ModuleException {
    return fileItem != null ? fileItem.createInputStream() : null;
  }

  /**
   * @return checks if an inputstream can be created
   */
  public boolean canCreateInputstream() {
    return fileItem != null;
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
  public void cleanResources() throws IOException {
    fileItem.delete();
  }

  @Override
  public String toString() {
    return "BinaryCell{" + "fileItem=" + fileItem + '}';
  }
}
