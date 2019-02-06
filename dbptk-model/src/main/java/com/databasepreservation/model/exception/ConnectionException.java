package com.databasepreservation.model.exception;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ConnectionException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Problem in database connection. ";
  }
}
