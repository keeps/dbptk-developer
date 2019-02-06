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
 * Exception thrown when a database type is not known
 *
 * @author Luis Faria
 */
public class UnknownTypeException extends ModuleException {
  public UnknownTypeException() {
    super();
  }

  public UnknownTypeException(String message) {
    this();
    withMessage(message);
  }
}
