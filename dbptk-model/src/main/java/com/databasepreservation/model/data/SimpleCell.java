/**
 *
 */
package com.databasepreservation.model.data;

import java.io.UnsupportedEncodingException;

/**
 * Container of simple data
 *
 * @author Luis Faria
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SimpleCell extends Cell {
  private final String simpleData;
  private Long size;

  /**
   * Simple cell constructor with empty data. This should not be used to represent
   * NULL, instead a NullCell should be created.
   *
   * @param id
   *          the cell id
   */
  protected SimpleCell(String id) {
    super(id);
    simpleData = null;
  }

  /**
   * Simple cell constructor
   *
   * @param id
   *          the cell id, equal to 'tableId.columnName.rowIndex'
   * @param simpleData
   *          the content of the cell
   */
  public SimpleCell(String id, String simpleData) {
    super(id);
    this.simpleData = simpleData;
  }

  /**
   * @return the content of the cell
   */
  public String getSimpleData() {
    return simpleData;
  }

  /**
   * Gets the size of the simpleData in bytes (which may be different from the
   * string length measured in characters) using UTF-8 encoding
   * 
   * @return the size of the string in bytes; -1 is returned if the string is null
   */
  public long getBytesSize() {
    return getBytesSize(null);
  }

  /**
   * Gets the size of the simpleData in bytes (which may be different from the
   * string length measured in characters)
   * 
   * @param encoding
   *          the encoding to use when getting the string size in bytes; if null,
   *          UTF-8 encoding is used
   * @return the size of the string in bytes; -1 is returned if the string is null
   */
  public long getBytesSize(String encoding) {
    // if the size is known, return it
    if (size != null) {
      return size;
    }

    // if the data is null, its length is -1
    if (simpleData == null) {
      size = -1L;
      return size;
    }

    // if the encoding is null, use UTF-8 by default
    if (encoding == null) {
      encoding = "UTF-8";
    }

    // avoid converting to byte[] for encodings with known character size
    try {
      switch (encoding) {
        case "US-ASCII":
        case "ISO-8859-1":
          // 1 char = 1 byte
          size = (long) simpleData.length();
          break;
        case "UTF-16BE":
        case "UTF-16LE":
        case "UTF-16":
          // 1 char = 2 bytes
          size = (long) simpleData.length() * 2;
          break;
        case "UTF-8":
        default:
          size = (long) simpleData.getBytes(encoding).length;
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Invalid encoding: " + encoding, e);
    }

    return size;
  }
}
