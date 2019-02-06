package com.databasepreservation.model.exception;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ServerException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Problem in database server. ";
  }
}
