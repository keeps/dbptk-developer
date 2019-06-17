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