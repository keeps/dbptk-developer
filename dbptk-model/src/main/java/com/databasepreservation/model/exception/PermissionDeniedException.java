package com.databasepreservation.model.exception;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PermissionDeniedException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Permission denied. ";
  }
}
