package com.databasepreservation.model.exception;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class UnsupportedModuleException extends ModuleException {
  /**
   * Create a generic module exception
   *
   * @param mesg
   *          the error message
   */
  public UnsupportedModuleException(String mesg) {
    super(mesg);
  }
}
