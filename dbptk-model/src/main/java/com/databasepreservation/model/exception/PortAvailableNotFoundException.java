package com.databasepreservation.model.exception;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class PortAvailableNotFoundException extends ModuleException {

  public PortAvailableNotFoundException() {
    super();
  }

  public PortAvailableNotFoundException(String message) {
    this();
    withMessage(message);
  }
}
