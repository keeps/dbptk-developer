/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.structure.type;

/**
 * An approximate numeric is essentially a floating point and for each a
 * precision may be optionally specified.
 *
 * @author Luis Faria
 */
public class SimpleTypeNumericApproximate extends Type {

  private Integer precision;

  /**
   * Aproximate numeric, like floating point
   */
  public SimpleTypeNumericApproximate() {

  }

  /**
   * Approximate numeric, like floating point, with optional fields.
   *
   * @param precision
   *          the number of digits (optional)
   */
  public SimpleTypeNumericApproximate(int precision) {
    this.precision = precision;
  }

  /**
   * @return the number of digits, or null if undefined
   */
  public Integer getPrecision() {
    return precision;
  }

  /**
   * @param precision
   *          the number of digits, or null if undefined
   */
  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((precision == null) ? 0 : precision.hashCode());
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
    SimpleTypeNumericApproximate other = (SimpleTypeNumericApproximate) obj;
    if (precision == null) {
      if (other.precision != null) {
        return false;
      }
    } else if (!precision.equals(other.precision)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return super.toString() + "-->SimpleTypeNumericApproximate{" + "precision=" + precision + '}';
  }
}
