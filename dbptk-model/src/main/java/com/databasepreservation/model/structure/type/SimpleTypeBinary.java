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
package com.databasepreservation.model.structure.type;

/**
 * A value of binary string type (known as a binary large object, or BLOB) is a
 * variable length sequence of octets, up to an implementation-defined maximum.
 *
 * @author Luis Faria
 */
public class SimpleTypeBinary extends Type {
  private String formatRegistryName;

  private String formatRegistryKey;

  private Integer length;

  /**
   * Binary type constructor, with no optional fields. Format registry name and
   * key will be null
   */
  public SimpleTypeBinary() {
    formatRegistryName = null;
    formatRegistryKey = null;
    length = null;
  }

  /**
   * @param length
   *          Column size
   */
  public SimpleTypeBinary(Integer length) {
    formatRegistryName = null;
    formatRegistryKey = null;
    this.length = length;
  }

  /**
   * Binary type constructor, with optional fields
   *
   * @param formatRegistryName
   *          a file format registry, like MIME or PRONOM
   * @param formatRegistryKey
   *          the file format according to the designated registry, e.g. image/png
   *          for MIME Type
   */
  public SimpleTypeBinary(String formatRegistryName, String formatRegistryKey) {
    this.formatRegistryName = formatRegistryName;
    this.formatRegistryKey = formatRegistryKey;
    this.length = null;
  }

  /**
   * @return the file format registry, like MIME or PRONOM
   */
  public String getFormatRegistryKey() {
    return formatRegistryKey;
  }

  /**
   * @return the file format according to the designated registry, e.g. image/png
   *         for MIME Type
   */
  public String getFormatRegistryName() {
    return formatRegistryName;
  }

  /**
   * The file format according to a designated registry
   *
   * @param name
   *          the name of the registry, like MIME or PRONOM
   * @param key
   *          the file format, like image/png for MIME
   */
  public void setFormatRegistry(String name, String key) {
    this.formatRegistryName = name;
    this.formatRegistryKey = key;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((formatRegistryKey == null) ? 0 : formatRegistryKey.hashCode());
    result = prime * result + ((formatRegistryName == null) ? 0 : formatRegistryName.hashCode());
    result = prime * result + ((length == null) ? 0 : length.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SimpleTypeBinary other = (SimpleTypeBinary) obj;
    if (formatRegistryKey == null) {
      if (other.formatRegistryKey != null) {
        return false;
      }
    } else if (!formatRegistryKey.equals(other.formatRegistryKey)) {
      return false;
    }
    if (formatRegistryName == null) {
      if (other.formatRegistryName != null) {
        return false;
      }
    } else if (!formatRegistryName.equals(other.formatRegistryName)) {
      return false;
    }
    if (length == null) {
      if (other.length != null) {
        return false;
      }
    } else if (!length.equals(other.length)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return super.toString() + "-->SimpleTypeBinary{" + "formatRegistryName='" + formatRegistryName + '\''
      + ", formatRegistryKey='" + formatRegistryKey + '\'' + ", length=" + length + '}';
  }
}
