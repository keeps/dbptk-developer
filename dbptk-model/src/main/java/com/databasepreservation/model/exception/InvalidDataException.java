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
  public InvalidDataException() {
    super();
  }

  public InvalidDataException(String mesg) {
    this();
    withMessage(mesg);
  }
}
