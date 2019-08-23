package com.databasepreservation.common;

import com.databasepreservation.model.reporters.ValidationReporter.Status;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidationObserver {
  void notifyStartValidationModule(String moduleName, String ID);

  void notifyValidationStep(String moduleName, String step, Status status);

  void notifyMessage(String moduleName, String message, Status status);

  void notifyFinishValidationModule(String moduleName, Status status);
}
