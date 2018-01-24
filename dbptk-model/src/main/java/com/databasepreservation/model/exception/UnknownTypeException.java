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
