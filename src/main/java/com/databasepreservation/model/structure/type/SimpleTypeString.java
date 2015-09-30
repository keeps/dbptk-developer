/**
 *
 */
package com.databasepreservation.model.structure.type;

/**
 * Sequence of characters drawn from character repertoire (charset) This
 * sequence is either of fixed length, or of variable length up to some
 * implementation-defined maximum.
 *
 * @author Luis Faria
 */
public class SimpleTypeString extends Type {
  private Integer length;

  private Boolean lengthVariable;

  private String charset;

  /**
   * String type constructor with only required fields
   *
   * @param length
   *          the maximum string length (required)
   * @param lengthVariable
   *          true if the string size can vary up to the maximum length, false
   *          if it fixed to length (required)
   */
  public SimpleTypeString(Integer length, Boolean lengthVariable) {
    this.length = length;
    this.lengthVariable = lengthVariable;
  }

  /**
   * String type constructor with required and optional fields. Insert null in
   * optional fields to make them undefined.
   *
   * @param length
   *          the maximum string length (required)
   * @param lengthVariable
   *          true if the string size can vary up to the maximum length, false
   *          if it fixed to length (required)
   * @param charset
   *          the character repertoire used to create the string, e.g. UTF-8
   *          (optional)
   */
  public SimpleTypeString(Integer length, Boolean lengthVariable, String charset) {
    this.length = length;
    this.lengthVariable = lengthVariable;
    this.charset = charset;
  }

  /**
   * @return the character repertoire used to create the string, e.g. UTF-8
   */
  public String getCharset() {
    return charset;
  }

  /**
   * @param charset
   *          the character repertoire used to create the string, e.g. UTF-8
   */
  public void setCharset(String charset) {
    this.charset = charset;
  }

  /**
   * @return the maximum string length
   */
  public Integer getLength() {
    return length;
  }

  /**
   * @param length
   *          the maximum string length
   */
  public void setLength(Integer length) {
    this.length = length;
  }

  /**
   * @return true if the string size can vary up to the maximum length, false if
   *         it fixed to lenght
   */
  public Boolean isLengthVariable() {
    return lengthVariable;
  }

  /**
   * @param lengthVariable
   *          true if the string size can vary up to the maximum length, false
   *          if it fixed to lenght
   */
  public void setLengthVariable(Boolean lengthVariable) {
    this.lengthVariable = lengthVariable;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((charset == null) ? 0 : charset.hashCode());
    result = prime * result + ((length == null) ? 0 : length.hashCode());
    result = prime * result + ((lengthVariable == null) ? 0 : lengthVariable.hashCode());
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
    SimpleTypeString other = (SimpleTypeString) obj;
    if (charset == null) {
      if (other.charset != null) {
        return false;
      }
    } else if (!charset.equals(other.charset)) {
      return false;
    }
    if (length == null) {
      if (other.length != null) {
        return false;
      }
    } else if (!length.equals(other.length)) {
      return false;
    }
    if (lengthVariable == null) {
      if (other.lengthVariable != null) {
        return false;
      }
    } else if (!lengthVariable.equals(other.lengthVariable)) {
      return false;
    }
    return true;
  }

}
