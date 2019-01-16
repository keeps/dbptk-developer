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
 * A value of the Boolean data type is either true or false. The truth value of
 * unknown is sometimes represented by the null value.
 *
 * @author Luis Faria
 */
public class SimpleTypeBoolean extends Type {

  /**
   * Boolean type constructor
   */
  public SimpleTypeBoolean() {
  }

  @Override
  public String toString() {
    return super.toString() + "-->SimpleTypeBoolean{}";
  }
}
