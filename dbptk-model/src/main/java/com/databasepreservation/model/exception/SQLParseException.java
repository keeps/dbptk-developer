package com.databasepreservation.model.exception;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SQLParseException extends ModuleException {

  public SQLParseException() {
    super();
  }

  public SQLParseException(String message) {
    this();
    withMessage(message);
  }
}