/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TableNotFoundException extends ModuleException {

  public TableNotFoundException() {
    super();
  }

  public TableNotFoundException(String message) {
    this();
    withMessage(message);
  }
}