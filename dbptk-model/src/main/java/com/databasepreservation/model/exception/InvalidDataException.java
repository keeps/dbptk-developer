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
package com.databasepreservation.model.exception;

/**
 * @author Luis Faria
 */
public class InvalidDataException extends ModuleException {
  private static final long serialVersionUID = -5706032629612775911L;

  /**
   * Invalid data exception empty constructor
   */
  public InvalidDataException() {
    super();
  }

  /**
   * Invalid data exception constructior
   *
   * @param mesg
   *          error message
   */
  public InvalidDataException(String mesg) {
    super(mesg);
  }

}
