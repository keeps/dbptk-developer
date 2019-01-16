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
 * Exception throwned when a original database type is unknown
 *
 * @author Luis Faria
 */
public class UnknownTypeException extends ModuleException {

  /**
         *
         */
  private static final long serialVersionUID = -4139481554575711876L;

  /**
   * Empty unknown type exception constructor
   */
  public UnknownTypeException() {
    super();
  }

  /**
   * unknown type exception constructor with message
   *
   * @param message
   *          the error message
   */
  public UnknownTypeException(String message) {
    super(message);
  }

}
